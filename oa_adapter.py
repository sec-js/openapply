#!/usr/bin/env python3
"""Open-Apply: ATS adapters → canonical JobPosting JSONL.

Usage:
  python3 oa_adapter.py --slug-dir slugs --out jobs.jsonl --limit 10
"""
import argparse, json, sys, time, urllib.request, urllib.error, urllib.parse, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone

UA = 'Mozilla/5.0 (open-apply/0.1)'
TIMEOUT = 20

@dataclass
class JobPosting:
    id: str                         # {source}:{slug}:{native_id}
    source: str                     # greenhouse|lever|ashby|rippling|gem|workday|smartrecruiters
    source_slug: str                # tenant slug on the ATS
    title: str
    apply_url: str
    description_html: Optional[str] = None
    employment_type: Optional[str] = None
    department: Optional[str] = None
    locations: List[str] = field(default_factory=list)
    remote: Optional[bool] = None
    posted_at: Optional[str] = None    # original publish/create timestamp, ISO 8601
    updated_at: Optional[str] = None   # last updated timestamp, ISO 8601 when exposed
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: Optional[str] = None
    salary_period: Optional[str] = None  # HOUR|DAY|WEEK|MONTH|YEAR

RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}
# Workday throttles sustained load per client IP with bare 429s (no Retry-After), and
# the short retries below gave up on ~14% of its sites, so 429s get a longer backoff.
RATE_LIMIT_RETRIES = 4   # waits 5, 10, 20, 40s

def _retry_wait(err: Exception, attempt: int, retries: int) -> Optional[float]:
    """Seconds to sleep before retrying after err, or None to give up."""
    if isinstance(err, urllib.error.HTTPError):
        if err.code not in RETRYABLE_STATUS: return None
        if err.code == 429:
            return 5 * 2 ** attempt if attempt < RATE_LIMIT_RETRIES else None
    return 1 + attempt if attempt < retries else None

def http_get(url: str, timeout: int = TIMEOUT, retries: int = 2) -> bytes:
    attempt = 0
    while True:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA, 'Accept': 'application/json'})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            wait = _retry_wait(e, attempt, retries)
            if wait is None: raise
            time.sleep(wait); attempt += 1

def http_json(url: str, timeout: int = TIMEOUT) -> Any:
    return json.loads(http_get(url, timeout))

# ---------- Greenhouse ----------
def fetch_greenhouse(slug: str) -> List[JobPosting]:
    url = f'https://boards-api.greenhouse.io/v1/boards/{urllib.parse.quote(slug)}/jobs?content=true'
    d = http_json(url)
    out = []
    for j in d.get('jobs', []):
        loc_name = (j.get('location') or {}).get('name') or ''
        locs = [loc_name] if loc_name else []
        remote = ('remote' in loc_name.lower()) if loc_name else None
        out.append(JobPosting(
            id=f"greenhouse:{slug}:{j.get('id')}",
            source='greenhouse', source_slug=slug,
            title=j.get('title') or '',
            apply_url=j.get('absolute_url') or '',
            description_html=j.get('content'),
            department=next((d.get('name') for d in (j.get('departments') or []) if isinstance(d, dict)), None),
            locations=locs, remote=remote,
            posted_at=j.get('first_published') or j.get('updated_at'),
            updated_at=j.get('updated_at'),
        ))
    return out

# ---------- Lever ----------
def fetch_lever(slug: str) -> List[JobPosting]:
    url = f'https://api.lever.co/v0/postings/{urllib.parse.quote(slug)}?mode=json&limit=500'
    d = http_json(url)
    out = []
    if not isinstance(d, list): return out
    for j in d:
        cat = j.get('categories') or {}
        sal = j.get('salaryRange') or {}
        out.append(JobPosting(
            id=f"lever:{slug}:{j.get('id')}",
            source='lever', source_slug=slug,
            title=j.get('text') or '',
            apply_url=j.get('hostedUrl') or j.get('applyUrl') or '',
            description_html=j.get('descriptionPlain') or j.get('description'),
            employment_type=cat.get('commitment'),
            department=cat.get('team') or cat.get('department'),
            locations=[cat.get('location')] if cat.get('location') else [],
            remote=('remote' in (cat.get('location','').lower())) if cat.get('location') else None,
            posted_at=_epoch_to_iso(j.get('createdAt')),
            updated_at=_epoch_to_iso(j.get('updatedAt')),
            salary_min=sal.get('min'),
            salary_max=sal.get('max'),
            salary_currency=sal.get('currency'),
            salary_period=(sal.get('interval') or '').upper() or None,
        ))
    return out

def _epoch_to_iso(ms):
    if not ms: return None
    try:
        return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(ms/1000.0))
    except (TypeError, ValueError, OSError):
        return None

# ---------- Ashby ----------
def fetch_ashby(slug: str) -> List[JobPosting]:
    url = f'https://api.ashbyhq.com/posting-api/job-board/{urllib.parse.quote(slug)}?includeCompensation=true'
    d = http_json(url)
    out = []
    for j in d.get('jobs', []):
        sec_locs = [l.get('location') if isinstance(l, dict) else l for l in (j.get('secondaryLocations') or [])]
        comp = (j.get('compensation') or {}).get('compensationTierSummary') or ''
        sal_min = sal_max = sal_curr = None
        # Parse "$180K - $220K USD" style strings — best-effort
        m = re.search(r'([$£€])\s*(\d[\d,.kKmM]*)\s*(?:-|–|to)\s*([$£€]?)\s*(\d[\d,.kKmM]*)\s*([A-Z]{3})?', comp)
        if m:
            sal_min = _parse_num(m.group(2))
            sal_max = _parse_num(m.group(4))
            sal_curr = m.group(5) or {'$':'USD','£':'GBP','€':'EUR'}.get(m.group(1))
        out.append(JobPosting(
            id=f"ashby:{slug}:{j.get('id')}",
            source='ashby', source_slug=slug,
            title=j.get('title') or '',
            apply_url=j.get('jobUrl') or f'https://jobs.ashbyhq.com/{slug}/{j.get("id")}',
            description_html=j.get('descriptionHtml') or j.get('description'),
            employment_type=j.get('employmentType'),
            department=j.get('department') or j.get('team'),
            locations=[j.get('location')] + sec_locs if j.get('location') else sec_locs,
            remote=j.get('isRemote'),
            posted_at=j.get('publishedAt') or j.get('updatedAt'),
            updated_at=j.get('updatedAt'),
            salary_min=sal_min, salary_max=sal_max,
            salary_currency=sal_curr,
        ))
    return out

def _parse_num(s: str) -> Optional[float]:
    s = s.replace(',', '').strip().lower()
    mul = 1
    if s.endswith('k'): mul, s = 1_000, s[:-1]
    elif s.endswith('m'): mul, s = 1_000_000, s[:-1]
    try: return float(s) * mul
    except ValueError: return None

# ---------- Rippling ----------
# The board list omits description, dates and pay; each job needs a detail fetch.
def fetch_rippling(slug: str) -> List[JobPosting]:
    base = f'https://ats.rippling.com/api/v2/board/{urllib.parse.quote(slug)}/jobs'
    items, page = [], 0
    while True:
        d = http_json(f'{base}?pageSize=100&page={page}')
        items += d.get('items') or []
        page += 1
        if page >= (d.get('totalPages') or 0): break
    def detail(it):
        try:
            return it, http_json(f'{base}/{urllib.parse.quote(it["id"])}')
        except urllib.error.HTTPError:
            return it, {}

    out = []
    with ThreadPoolExecutor(4) as ex:
        details = list(ex.map(detail, items))
    for it, j in details:
        desc = j.get('description') or {}
        locs = [l.get('name') for l in (it.get('locations') or []) if l.get('name')]
        kinds = {l.get('workplaceType') for l in (it.get('locations') or [])}
        pay = next(iter(j.get('payRangeDetails') or []), {})
        out.append(JobPosting(
            id=f"rippling:{slug}:{it.get('id')}",
            source='rippling', source_slug=slug,
            title=it.get('name') or '',
            apply_url=it.get('url') or f'https://ats.rippling.com/{slug}/jobs/{it.get("id")}',
            description_html=''.join(filter(None, [desc.get('company'), desc.get('role')])) or None,
            employment_type=(j.get('employmentType') or {}).get('id'),
            department=(it.get('department') or {}).get('name'),
            locations=locs,
            remote=('REMOTE' in kinds) if kinds - {None} else None,
            posted_at=j.get('createdOn'),
            salary_min=pay.get('rangeStart'), salary_max=pay.get('rangeEnd'),
            salary_currency=pay.get('currency'), salary_period=pay.get('frequency'),
        ))
    return out

# ---------- Gem ----------
def fetch_gem(slug: str) -> List[JobPosting]:
    url = f'https://api.gem.com/job_board/v0/{urllib.parse.quote(slug)}/job_posts/'
    d = http_json(url)
    out = []
    if not isinstance(d, list): return out
    for j in d:
        offices = [((o.get('location') or {}).get('name')) for o in (j.get('offices') or [])]
        loc_name = (j.get('location') or {}).get('name')
        locs = list(dict.fromkeys(filter(None, [loc_name] + offices)))
        out.append(JobPosting(
            id=f"gem:{slug}:{j.get('id')}",
            source='gem', source_slug=slug,
            title=j.get('title') or '',
            apply_url=j.get('absolute_url') or f'https://jobs.gem.com/{slug}/{j.get("id")}',
            description_html=j.get('content'),
            employment_type=j.get('employment_type'),
            department=next((d.get('name') for d in (j.get('departments') or []) if isinstance(d, dict)), None),
            locations=locs,
            remote=(j.get('location_type') == 'remote') if j.get('location_type') else None,
            posted_at=j.get('first_published_at') or j.get('created_at'),
            updated_at=j.get('updated_at'),
        ))
    return out

# ---------- Workday ----------
# Slug is tenant/wdN/site. The list API only gives title, location and a relative
# "Posted N Days Ago", newest first, so page until postings age past WORKDAY_MAX_AGE and
# only fetch details (description, start date) for titles the shortlist would keep.
# With FULL_BOARDS (the published dataset) every listed posting is kept, and details
# are fetched only for postings from the last FULL_BOARDS_DETAIL_DAYS days, so each
# job gets its description once, in the daily partition where it first appears.
WORKDAY_MAX_AGE = 30
WORKDAY_MAX_PAGES = 50
WORKDAY_LIST_CAP = 2000   # the unfiltered list API repeats itself past offset 2000
FULL_BOARDS = False
FULL_BOARDS_DETAIL_DAYS = 2

def http_post_json(url: str, body: dict, timeout: int = TIMEOUT, retries: int = 2) -> Any:
    attempt = 0
    while True:
        try:
            req = urllib.request.Request(url, data=json.dumps(body).encode(), method='POST', headers={
                'User-Agent': UA, 'Accept': 'application/json', 'Content-Type': 'application/json'})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            wait = _retry_wait(e, attempt, retries)
            if wait is None: raise
            time.sleep(wait); attempt += 1

def _workday_age_days(posted_on: str) -> Optional[int]:
    s = (posted_on or '').lower()
    if 'today' in s: return 0
    if 'yesterday' in s: return 1
    m = re.search(r'(\d+)\+?\s*days', s)
    return int(m.group(1)) + (1 if '+' in s else 0) if m else None

_role_re = None
def _workday_role_like(title: str) -> bool:
    global _role_re
    if _role_re is None:
        sys.path.insert(0, 'scripts')
        from build_shortlist import ROLE_RE
        _role_re = ROLE_RE
    return bool(_role_re.search(title))

def fetch_workday(slug: str) -> List[JobPosting]:
    tenant, dc, site = slug.split('/')
    host = f'https://{tenant}.{dc}.myworkdayjobs.com'
    base = f'{host}/wday/cxs/{tenant}/{site}'
    postings, total = [], None
    for page in range(WORKDAY_LIST_CAP // 20 if FULL_BOARDS else WORKDAY_MAX_PAGES):
        d = http_post_json(f'{base}/jobs', {'limit': 20, 'offset': page * 20, 'searchText': '', 'appliedFacets': {}})
        batch = d.get('jobPostings') or []
        postings += batch
        total = total or d.get('total')   # only reported on the first page
        ages = [_workday_age_days(p.get('postedOn')) for p in batch]
        if len(batch) < 20 or (total and len(postings) >= total):
            break
        if not FULL_BOARDS and ages and all(a is not None and a > WORKDAY_MAX_AGE for a in ages):
            break
    postings = [p for p in {p.get('externalPath'): p for p in postings}.values() if p.get('externalPath')]
    if FULL_BOARDS:
        wanted = [p for p in postings if (_workday_age_days(p.get('postedOn')) or 0) <= FULL_BOARDS_DETAIL_DAYS]
    else:
        wanted = [p for p in postings if _workday_role_like(p.get('title') or '')
                  and (_workday_age_days(p.get('postedOn')) or 0) <= WORKDAY_MAX_AGE]

    def detail(p):
        try:
            return p, http_json(f'{base}{p["externalPath"]}').get('jobPostingInfo') or {}
        except urllib.error.HTTPError:
            return p, None
        except Exception:
            if not FULL_BOARDS: raise
            return p, {}   # keep the listed row without a description

    out = []
    with ThreadPoolExecutor(4) as ex:
        details = dict(ex.map(lambda p: (p['externalPath'], detail(p)[1]), wanted))
    today = datetime.now(timezone.utc).date()
    for p in postings:
        j = details.get(p['externalPath'], {} if FULL_BOARDS else None)
        if j is None or j.get('canApply') is False: continue
        if not j:
            age = _workday_age_days(p.get('postedOn'))
            posted = None if age is None or '+' in (p.get('postedOn') or '') else (today - timedelta(days=age)).isoformat()
            out.append(JobPosting(
                id=f"workday:{tenant}:{p['externalPath']}",
                source='workday', source_slug=slug,
                title=p.get('title') or '',
                apply_url=f'{host}/{site}{p["externalPath"]}',
                locations=[p['locationsText']] if p.get('locationsText') else [],
                remote=('remote' in p['locationsText'].lower() or None) if p.get('locationsText') else None,
                posted_at=posted,
            ))
            continue
        # Workday location labels are free text in either order ("Australia, WA,
        # Willowdale", "Hyderabad - Phoenix Equinox Tower 2") and misread as US
        # state codes / cities, so suffix the primary one with its real country.
        country = (j.get('country') or (j.get('jobRequisitionLocation') or {}).get('country') or {}).get('descriptor')
        primary = j.get('location') or p.get('locationsText')
        if primary and country and country.lower() not in primary.lower():
            primary = f'{primary}, {country}'
        locs = [l for l in [primary] + (j.get('additionalLocations') or []) if l]
        out.append(JobPosting(
            # Published rows key on externalPath so a job keeps one id whether or not
            # its details were fetched that day; local ids stay as the ledger has them.
            id=f"workday:{tenant}:{p['externalPath'] if FULL_BOARDS else j.get('id') or p['externalPath']}",
            source='workday', source_slug=slug,
            title=j.get('title') or p.get('title') or '',
            apply_url=j.get('externalUrl') or f'{host}/{site}{p["externalPath"]}',
            description_html=j.get('jobDescription'),
            employment_type=j.get('timeType'),
            locations=locs,
            remote=any('remote' in l.lower() for l in locs) or None,
            posted_at=j.get('startDate'),
        ))
    return out

# ---------- SmartRecruiters ----------
# The postings list has title, location and releasedDate but no description, so
# (as for Workday) only fetch details for recent, role-like titles, or with
# FULL_BOARDS keep every posting and fetch details for the newest ones.
SMARTRECRUITERS_MAX_AGE = 30

def fetch_smartrecruiters(slug: str) -> List[JobPosting]:
    base = f'https://api.smartrecruiters.com/v1/companies/{urllib.parse.quote(slug)}/postings'
    cutoff = time.time() - (FULL_BOARDS_DETAIL_DAYS if FULL_BOARDS else SMARTRECRUITERS_MAX_AGE) * 86400
    postings, offset = [], 0
    while True:
        d = http_json(f'{base}?limit=100&offset={offset}')
        batch = d.get('content') or []
        postings += batch
        offset += len(batch)
        if not batch or offset >= (d.get('totalFound') or 0): break

    def recent(p):
        try:
            return datetime.fromisoformat(p['releasedDate'].replace('Z', '+00:00')).timestamp() >= cutoff
        except (KeyError, TypeError, ValueError):
            return True

    if FULL_BOARDS:
        wanted = [p for p in postings if recent(p)]
    else:
        wanted = [p for p in postings if recent(p) and _workday_role_like(
            f"{p.get('name') or ''} {(p.get('department') or {}).get('label') or ''}")]

    def detail(p):
        try:
            return p, http_json(f'{base}/{urllib.parse.quote(str(p["id"]))}')
        except urllib.error.HTTPError:
            return p, None
        except Exception:
            if not FULL_BOARDS: raise
            return p, {}

    out = []
    with ThreadPoolExecutor(4) as ex:
        details = dict((p['id'], j) for p, j in ex.map(detail, wanted))
    for p in postings if FULL_BOARDS else wanted:
        j = details.get(p['id'], {})
        if j is None or j.get('active') is False: continue
        loc = j.get('location') or p.get('location') or {}
        place = loc.get('fullLocation') or ', '.join(filter(None, [loc.get('city'), loc.get('region'), (loc.get('country') or '').upper()]))
        sections = ((j.get('jobAd') or {}).get('sections') or {})
        out.append(JobPosting(
            id=f"smartrecruiters:{slug}:{p.get('id')}",
            source='smartrecruiters', source_slug=slug,
            title=p.get('name') or '',
            apply_url=j.get('postingUrl') or f'https://jobs.smartrecruiters.com/{slug}/{p.get("id")}',
            description_html=''.join((sections.get(k) or {}).get('text') or '' for k in
                                     ('jobDescription', 'qualifications', 'additionalInformation')) or None,
            employment_type=(p.get('typeOfEmployment') or {}).get('label'),
            department=(p.get('department') or {}).get('label'),
            locations=[place] if place else [],
            remote=loc.get('remote'),
            posted_at=p.get('releasedDate'),
        ))
    return out

ADAPTERS = {
    'greenhouse': fetch_greenhouse,
    'lever':      fetch_lever,
    'ashby':      fetch_ashby,
    'rippling':   fetch_rippling,
    'gem':        fetch_gem,
    'workday':    fetch_workday,
    'smartrecruiters': fetch_smartrecruiters,
}

def run_adapter(ats: str, slug: str) -> tuple:
    try:
        jobs = ADAPTERS[ats](slug)
        return (ats, slug, jobs, None)
    except Exception as e:
        return (ats, slug, [], f'{type(e).__name__}: {e}')

def _iso_utc(ts: str) -> str:
    try:
        d = datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except ValueError:
        return ''
    if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--slug-dir', default='slugs', help='dir with cc_{ats}_FINAL.txt files')
    ap.add_argument('--out', default='oa_jobs.jsonl')
    ap.add_argument('--limit', type=int, default=0, help='max slugs per ATS (0=all)')
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--ats', default=','.join(ADAPTERS))
    ap.add_argument('--suffix', default='FINAL', help='slug file suffix (FINAL or NEW)')
    ap.add_argument('--full-boards', action='store_true',
                    help='keep every Workday/SmartRecruiters posting (for the published dataset); '
                         'details only for postings from the last FULL_BOARDS_DETAIL_DAYS days')
    ap.add_argument('--max-age-days', type=int, default=0,
                    help='keep only postings with posted_at within the last N days (0=no filter)')
    args = ap.parse_args()
    global FULL_BOARDS, WORKDAY_MAX_AGE, SMARTRECRUITERS_MAX_AGE
    FULL_BOARDS = args.full_boards
    if args.max_age_days:
        WORKDAY_MAX_AGE = SMARTRECRUITERS_MAX_AGE = args.max_age_days
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=args.max_age_days)).isoformat() if args.max_age_days else None

    tasks = []
    for ats in args.ats.split(','):
        path = f'{args.slug_dir}/cc_{ats}_{args.suffix}.txt'
        try:
            slugs = [ln.strip() for ln in open(path) if ln.strip()]
        except FileNotFoundError:
            print(f'WARN: no slug file for {ats} at {path}', file=sys.stderr); continue
        if args.limit: slugs = slugs[:args.limit]
        for s in slugs: tasks.append((ats, s))
    # Longest jobs first: Workday and Rippling tenants each need many detail calls, and
    # queued last they left the pool idling on a long tail (2h fetch, 2026-09-23).
    slow_first = {'workday': 0, 'rippling': 1, 'smartrecruiters': 2}
    tasks.sort(key=lambda task: slow_first.get(task[0], 3))

    print(f'tasks: {len(tasks)} total, workers={args.workers}', file=sys.stderr)
    ok = err = jobs_total = 0
    by_ats = {}
    with open(args.out, 'w') as out_f, ThreadPoolExecutor(args.workers) as ex:
        futs = [ex.submit(run_adapter, ats, slug) for ats, slug in tasks]
        for i, f in enumerate(as_completed(futs), 1):
            ats, slug, jobs, err_msg = f.result()
            by_ats.setdefault(ats, {'tenants_ok':0, 'tenants_err':0, 'jobs':0})
            if err_msg:
                err += 1; by_ats[ats]['tenants_err'] += 1
                if by_ats[ats]['tenants_err'] <= 10: print(f'  ERR {ats}:{slug} → {err_msg}', file=sys.stderr)
            else:
                ok += 1; by_ats[ats]['tenants_ok'] += 1
                for jp in jobs:
                    if cutoff_iso and not (jp.posted_at and _iso_utc(jp.posted_at) >= cutoff_iso): continue
                    out_f.write(json.dumps(asdict(jp)) + '\n')
                    jobs_total += 1
                    by_ats[ats]['jobs'] += 1
            if i % 50 == 0 or i == len(tasks):
                print(f'  progress {i}/{len(tasks)} ok={ok} err={err} jobs={jobs_total}', file=sys.stderr)

    print(f'\nDONE: {ok} tenants ok, {err} tenants err, {jobs_total} jobs written to {args.out}')
    print(f'{"ATS":<16}{"ok":>6}{"err":>6}{"jobs":>10}')
    for ats, s in by_ats.items():
        print(f'{ats:<16}{s["tenants_ok"]:>6}{s["tenants_err"]:>6}{s["jobs"]:>10}')

if __name__ == '__main__':
    main()
