import React from 'react';
import { Info } from 'lucide-react';

export default function AlgorithmicEstimateBanner({ className = '' }) {
  return (
    <div
      className={`flex items-start sm:items-center gap-2 rounded-lg border border-amber-200/80 bg-amber-50/75 px-3 py-1.5 text-[11px] font-medium text-amber-900 ${className}`}
    >
      <Info size={13} className="shrink-0 mt-0.5 sm:mt-0 text-amber-600" aria-hidden="true" />
      <span>
        <strong className="font-bold">Algorithmic estimate — verify primary records:</strong> Network groupings are automated research leads built from public filings; listed people may be corporate officers, managers, or co-filers rather than beneficial owners. Verify links, counts, and enforcement records against primary municipal, registry, and court sources before relying on them in legal filings or advocacy.
      </span>
    </div>
  );
}
