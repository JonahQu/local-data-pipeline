#!/usr/bin/env python3
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent/'core'))
import pipeline as p

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--source',type=Path,default=p.SOURCE)
    ap.add_argument('--root',type=Path,default=p.ROOT)
    a=ap.parse_args(); root=a.root.resolve()
    result={'ingest':p.ingest(root,a.source),'summaries':[]}
    for _ in range(20):
        row=p.summarize(root,limit=50)
        if not row.get('summary_created'):break
        result['summaries'].append(row)
    else:result['more_summary_work_remains']=True
    with p.writer(root):result['publish']=p.publish(root)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return 2 if result['ingest']['state']=='partial' else 0
if __name__=='__main__':sys.exit(main())
