import React from 'react';
import { Info } from 'lucide-react';

export default function AlgorithmicEstimateBanner({ className = '' }) {
  return (
    <div
      className={`flex items-start sm:items-center gap-2 rounded-lg border border-amber-200/80 bg-amber-50/75 px-3 py-1.5 text-[11px] font-medium text-amber-900 ${className}`}
    >
      <Info size={13} className="shrink-0 mt-0.5 sm:mt-0 text-amber-600" aria-hidden="true" />
      <span>
        <strong className="font-bold">Algorithmic estimate:</strong> Network associations are algorithmic estimates built from public records; ongoing daily data ingestion and linkage refinements can shift counts slightly.
      </span>
    </div>
  );
}
