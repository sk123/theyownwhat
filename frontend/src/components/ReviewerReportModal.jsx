import React, { useEffect, useId, useRef, useState } from 'react';
import { CheckCircle2, Flag, Link2, Loader2, ShieldAlert, X } from 'lucide-react';
import { useReviewerMode } from '../context/ReviewerModeContext';

const JURISDICTION_OPTIONS = [
  ['ct', 'Connecticut (CT)'],
  ['ny', 'New York (NY / NYC)'],
  ['philadelphia', 'Philadelphia (PA)'],
  ['fl', 'Florida (FL)'],
  ['ma', 'Massachusetts (MA)'],
  ['il', 'Illinois (IL)'],
  ['ca', 'LA & San Francisco (Partial CA)'],
  ['nj', 'New Jersey (NJ)'],
  ['dc', 'Washington, D.C.'],
  ['baltimore', 'Baltimore (MD)'],
  ['detroit', 'Detroit (MI)'],
  ['mn', 'Minnesota (MN)'],
  ['oh', 'Ohio (OH)'],
  ['va', 'Virginia (VA)'],
  ['nc', 'North Carolina (NC)'],
  ['co', 'Colorado (CO)'],
  ['wi', 'Wisconsin (WI)'],
  ['ar', 'Arkansas (AR)'],
  ['vt', 'Vermont (VT)'],
  ['mt', 'Montana (MT)'],
];

const FALSE_POSITIVE_REASONS = [
  { code: 'wrong_person', label: 'Person does not belong in this network' },
  { code: 'same_name_different_person', label: 'Different person with the same name (homonym collision)' },
  { code: 'third_party_intermediary', label: 'Third-party attorney, CPA, registered agent, or property manager' },
  { code: 'wrong_entity', label: 'Entity / LLC does not belong in this network' },
  { code: 'shared_office_or_maildrop', label: 'Shared office tower, UPS/PMB maildrop, or multi-tenant address' },
  { code: 'nominee_manager_factory', label: 'Commercial nominee manager / corporate formation factory' },
  { code: 'non_landlord_entity', label: 'Non-landlord (co-op, HOA, bank, utility, nonprofit, school, govt)' },
  { code: 'stale_or_sold_property', label: 'Stale or sold property / outdated officer record' },
  { code: 'wrong_label', label: 'Misleading representative network label' },
  { code: 'other', label: 'Other linkage error' },
];

const MISSED_CONNECTION_REASONS = [
  { code: 'missed_shared_address', label: 'Share same office / mailing address (spelling, ZIP, or C/O variant)' },
  { code: 'missed_overblocked_hq', label: 'Real landlord HQ appears over-blocked as a registered agent' },
  { code: 'missed_shared_principal', label: 'Share same owner / officer / principal (name or title suffix variation)' },
  { code: 'missed_corporate_affiliate', label: 'Same corporate group / abbreviation / parent-subsidiary mismatch' },
  { code: 'missed_shared_email', label: 'Share same business email or custom domain' },
  { code: 'missed_false_exclusion', label: 'Private landlord LLC falsely excluded as institutional / non-landlord' },
  { code: 'missed_connection_other', label: 'Other missed real connection' },
];

const MISSING_TARGET_TYPES = [
  { value: 'entity', label: 'Entity / LLC' },
  { value: 'principal', label: 'Person / Principal' },
  { value: 'property', label: 'Property Address' },
  { value: 'network', label: 'Other Network' },
];

function defaultReasonForDraft(reportKind, targetType, explicitCode) {
  if (explicitCode) return explicitCode;
  if (reportKind === 'missed_connection') return 'missed_shared_address';
  switch (targetType) {
    case 'principal':
      return 'wrong_person';
    case 'address':
      return 'shared_office_or_maildrop';
    case 'property':
      return 'stale_or_sold_property';
    case 'label':
      return 'wrong_label';
    case 'entity':
    default:
      return 'wrong_entity';
  }
}

export default function ReviewerReportModal() {
  const {
    activeReportDraft,
    closeReportModal,
    reviewerLabel,
    setReviewerLabel,
    markItemFlagged,
  } = useReviewerMode();

  const dialogRef = useRef(null);
  const pressedOutsideRef = useRef(false);
  const sideAInputRef = useRef(null);
  const sideBInputRef = useRef(null);
  const explanationInputRef = useRef(null);
  const urlInputRef = useRef(null);

  const titleId = useId();
  const sideAErrorId = useId();
  const sideAHintId = useId();
  const sideBErrorId = useId();
  const sideBHintId = useId();
  const explanationErrorId = useId();
  const explanationHintId = useId();
  const urlErrorId = useId();
  const urlHintId = useId();

  const [reportKind, setReportKind] = useState('false_positive');
  const [jurisdiction, setJurisdiction] = useState('ct');
  const [sideAValue, setSideAValue] = useState('');
  const [relatedTargetType, setRelatedTargetType] = useState('entity');
  const [relatedTargetValue, setRelatedTargetValue] = useState('');
  const [relatedAddressClue, setRelatedAddressClue] = useState('');
  const [reasonCode, setReasonCode] = useState('wrong_entity');
  const [explanation, setExplanation] = useState('');
  const [evidenceUrl, setEvidenceUrl] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState('');
  const [submitSuccess, setSubmitSuccess] = useState(null);
  const [touchedFields, setTouchedFields] = useState({
    sideA: false,
    sideB: false,
    explanation: false,
    evidenceUrl: false,
  });

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;

    if (activeReportDraft) {
      const kind = activeReportDraft.reportKind === 'missed_connection' ? 'missed_connection' : 'false_positive';
      setReportKind(kind);
      setJurisdiction(activeReportDraft.jurisdiction || 'ct');
      setSideAValue(activeReportDraft.targetValue || activeReportDraft.networkName || '');
      setRelatedTargetType(activeReportDraft.relatedTargetType || 'entity');
      setRelatedTargetValue(activeReportDraft.relatedTargetValue || '');
      setRelatedAddressClue('');
      setReasonCode(defaultReasonForDraft(kind, activeReportDraft.targetType, activeReportDraft.defaultReasonCode));
      setExplanation('');
      setEvidenceUrl('');
      setSubmitError('');
      setSubmitSuccess(null);
      setTouchedFields({ sideA: false, sideB: false, explanation: false, evidenceUrl: false });

      if (!dialog.open) {
        try {
          dialog.showModal();
        } catch {
          dialog.setAttribute('open', '');
        }
      }
    } else if (dialog.open) {
      dialog.close();
    }
  }, [activeReportDraft]);

  const handleReportKindChange = (nextKind) => {
    setReportKind(nextKind);
    setReasonCode(defaultReasonForDraft(nextKind, activeReportDraft?.targetType, ''));
    setSubmitError('');
  };

  const handleClose = () => {
    const dialog = dialogRef.current;
    if (dialog) dialog.returnValue = '';
    closeReportModal();
  };

  // Mandatory light-dismiss fallback for browsers without native <dialog closedby="any">
  const isOutsideDialogBox = (event, dialog) => {
    const rect = dialog.getBoundingClientRect();
    return (
      event.clientX < rect.left ||
      event.clientX > rect.right ||
      event.clientY < rect.top ||
      event.clientY > rect.bottom
    );
  };

  const handlePointerDown = (event) => {
    if (typeof HTMLDialogElement !== 'undefined' && 'closedBy' in HTMLDialogElement.prototype) return;
    const dialog = dialogRef.current;
    if (!dialog) return;
    pressedOutsideRef.current = isOutsideDialogBox(event, dialog);
  };

  const handlePointerUp = (event) => {
    if (typeof HTMLDialogElement !== 'undefined' && 'closedBy' in HTMLDialogElement.prototype) return;
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (pressedOutsideRef.current && isOutsideDialogBox(event, dialog)) {
      dialog.close();
    }
    pressedOutsideRef.current = false;
  };

  const isMissed = reportKind === 'missed_connection';
  const sideATrimmed = sideAValue.trim();
  const sideBTrimmed = relatedTargetValue.trim();
  const urlTrimmed = evidenceUrl.trim();

  const sideAInvalid = touchedFields.sideA && sideATrimmed.length < 2;
  const sideBInvalid = isMissed && touchedFields.sideB && sideBTrimmed.length < 2;
  const explanationInvalid = touchedFields.explanation && explanation.trim().length < 6;
  const urlInvalid =
    touchedFields.evidenceUrl &&
    urlTrimmed.length > 0 &&
    !(urlTrimmed.startsWith('https://') || urlTrimmed.startsWith('http://'));

  const handleSubmit = async (event) => {
    event.preventDefault();
    if (!activeReportDraft) return;
    setTouchedFields({ sideA: true, sideB: true, explanation: true, evidenceUrl: true });
    setSubmitError('');

    if (sideATrimmed.length < 2) {
      setSubmitError('Please enter the primary network, entity, or person.');
      sideAInputRef.current?.focus();
      return;
    }
    if (isMissed && sideBTrimmed.length < 2) {
      setSubmitError('Please enter the missing entity, person, property, or network that should connect.');
      sideBInputRef.current?.focus();
      return;
    }
    if (explanation.trim().length < 6) {
      setSubmitError('Please briefly explain the public-record clue (at least 6 characters).');
      explanationInputRef.current?.focus();
      return;
    }
    if (urlTrimmed && !(urlTrimmed.startsWith('https://') || urlTrimmed.startsWith('http://'))) {
      setSubmitError('Evidence URL must start with https:// or http://.');
      urlInputRef.current?.focus();
      return;
    }

    const mergedContext = {
      ...(activeReportDraft.bridgeContext || {}),
      ...(relatedAddressClue.trim() ? { related_address: relatedAddressClue.trim() } : {}),
    };

    setSubmitting(true);
    try {
      const res = await fetch('/api/review-reports', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          report_kind: reportKind,
          jurisdiction,
          network_id: activeReportDraft.networkId,
          network_key: activeReportDraft.networkKey,
          network_name: activeReportDraft.networkName || sideATrimmed,
          target_type: isMissed ? (activeReportDraft.networkId || activeReportDraft.networkKey ? 'network' : activeReportDraft.targetType || 'entity') : activeReportDraft.targetType,
          target_value: sideATrimmed,
          target_id: activeReportDraft.targetId,
          related_target_type: isMissed ? relatedTargetType : '',
          related_target_value: isMissed ? sideBTrimmed : '',
          related_target_id: isMissed ? activeReportDraft.relatedTargetId : '',
          bridge_context: mergedContext,
          reason_code: reasonCode,
          explanation: explanation.trim(),
          evidence_url: urlTrimmed,
          reviewer_label: reviewerLabel.trim(),
          source_path: activeReportDraft.sourcePath,
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data.detail || `Server responded with HTTP ${res.status}`);
      }
      markItemFlagged({
        ...activeReportDraft,
        reportKind,
        jurisdiction,
        targetValue: sideATrimmed,
        relatedTargetValue: isMissed ? sideBTrimmed : '',
      });
      setSubmitSuccess(data);
    } catch (err) {
      setSubmitError(err?.message || 'Unable to submit report right now.');
    } finally {
      setSubmitting(false);
    }
  };

  const draft = activeReportDraft;
  const reasonOptions = isMissed ? MISSED_CONNECTION_REASONS : FALSE_POSITIVE_REASONS;
  const hasLockedSideA = Boolean(draft?.targetValue && (!isMissed || draft?.networkId || draft?.networkKey || draft?.networkName));

  return (
    <dialog
      ref={dialogRef}
      {...({ closedby: 'any' })}
      aria-labelledby={titleId}
      onClose={handleClose}
      onPointerDown={handlePointerDown}
      onPointerUp={handlePointerUp}
      className="reviewer-report-dialog m-auto w-[min(94vw,40rem)] max-h-[88dvh] overflow-y-auto rounded-2xl border border-slate-200 bg-white p-0 text-slate-900 shadow-2xl"
    >
      {draft && (
        <div className="flex flex-col">
          <div className={`flex items-start justify-between gap-3 border-b border-slate-200 px-5 py-4 ${isMissed ? 'bg-indigo-50/80' : 'bg-amber-50/70'}`}>
            <div className="flex items-start gap-2.5 min-w-0">
              <div className={`mt-0.5 rounded-lg p-2 shrink-0 ${isMissed ? 'bg-indigo-100 text-indigo-800' : 'bg-amber-100 text-amber-800'}`}>
                {isMissed ? <Link2 size={16} aria-hidden="true" /> : <Flag size={16} aria-hidden="true" />}
              </div>
              <div className="min-w-0">
                <h2 id={titleId} className="text-base font-black tracking-tight text-slate-900">
                  {isMissed ? 'Report a Missed Real Connection' : 'Report Erroneous Network Inclusion'}
                </h2>
                <p className="mt-0.5 text-xs text-slate-600 truncate">
                  <strong className="uppercase">{jurisdiction}</strong>
                  {(draft.networkName || sideATrimmed) ? ` · ${draft.networkName || sideATrimmed}` : ''}
                </p>
              </div>
            </div>
            <button
              type="button"
              onClick={() => dialogRef.current?.close()}
              aria-label="Close report dialog"
              className="rounded-lg p-1.5 text-slate-500 hover:bg-black/5 hover:text-slate-900 transition-colors cursor-pointer shrink-0"
            >
              <X size={18} aria-hidden="true" />
            </button>
          </div>

          <div className="px-5 py-4 space-y-4">
            {submitSuccess ? (
              <div className="space-y-4 py-2" role="status" aria-live="polite">
                <div className="flex items-start gap-3 rounded-xl border border-emerald-200 bg-emerald-50 p-4 text-sm text-emerald-950">
                  <CheckCircle2 size={20} aria-hidden="true" className="text-emerald-600 shrink-0 mt-0.5" />
                  <div>
                    <div className="font-bold">
                      Report #{submitSuccess.report_id} queued for nightly evaluation
                    </div>
                    <p className="mt-1 text-xs leading-relaxed text-emerald-900">
                      {submitSuccess.message}
                    </p>
                  </div>
                </div>
                <div className="flex justify-end">
                  <button
                    type="button"
                    autoFocus
                    onClick={() => dialogRef.current?.close()}
                    className="rounded-xl bg-slate-900 px-4 py-2 text-xs font-bold text-white hover:bg-slate-800 cursor-pointer"
                  >
                    Done
                  </button>
                </div>
              </div>
            ) : (
              <form action="/api/review-reports" method="POST" onSubmit={handleSubmit} noValidate className="space-y-4">
                {/* Report Mode: 2 mutually exclusive options -> visible radio group inside <fieldset> + <legend> per modern-web-guidance `forms` */}
                <fieldset className="rounded-xl border border-slate-200 bg-slate-50/70 p-2.5">
                  <legend className="px-1 text-[10px] font-black uppercase tracking-wider text-slate-500">
                    Feedback Type
                  </legend>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-1">
                    <label className={`flex items-center gap-2 rounded-lg border px-3 py-2 cursor-pointer transition-colors ${!isMissed ? 'border-amber-400 bg-amber-50 text-amber-950 font-bold' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'}`}>
                      <input
                        type="radio"
                        name="report_kind"
                        value="false_positive"
                        checked={!isMissed}
                        onChange={() => handleReportKindChange('false_positive')}
                        className="accent-amber-600"
                      />
                      <span className="text-xs">Erroneous inclusion (should split)</span>
                    </label>
                    <label className={`flex items-center gap-2 rounded-lg border px-3 py-2 cursor-pointer transition-colors ${isMissed ? 'border-indigo-400 bg-indigo-50 text-indigo-950 font-bold' : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'}`}>
                      <input
                        type="radio"
                        name="report_kind"
                        value="missed_connection"
                        checked={isMissed}
                        onChange={() => handleReportKindChange('missed_connection')}
                        className="accent-indigo-600"
                      />
                      <span className="text-xs">Missed connection (should link)</span>
                    </label>
                  </div>
                </fieldset>

                {/* Jurisdiction selector when opened from global banner without a pre-locked network */}
                {!hasLockedSideA && (
                  <div>
                    <label htmlFor="reviewer-jur-select" className="block text-xs font-bold text-slate-700 mb-1">
                      Jurisdiction
                    </label>
                    <select
                      id="reviewer-jur-select"
                      name="jurisdiction"
                      value={jurisdiction}
                      onChange={(e) => setJurisdiction(e.target.value)}
                      className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-base sm:text-xs font-medium text-slate-900 focus:border-indigo-500 focus:outline-none"
                    >
                      {JURISDICTION_OPTIONS.map(([code, lbl]) => (
                        <option key={code} value={code}>{lbl}</option>
                      ))}
                    </select>
                  </div>
                )}

                {/* Side A Display or Editable Input */}
                {hasLockedSideA ? (
                  <div className="rounded-xl border border-slate-200 bg-slate-50 px-3.5 py-2.5 text-xs">
                    <div className="text-[10px] font-black uppercase tracking-wider text-slate-400">
                      {isMissed ? 'Starting Network / Entity (Side A)' : `Flagged ${draft.targetType}`}
                    </div>
                    <div className="mt-0.5 text-sm font-bold text-slate-900 break-words">
                      {sideATrimmed}
                    </div>
                  </div>
                ) : (
                  <div>
                    <label htmlFor="reviewer-side-a" className="block text-xs font-bold text-slate-700">
                      First entity, person, property, or network (Side A) <span className="text-rose-600" aria-hidden="true">*</span>
                    </label>
                    <span id={sideAHintId} className="block text-[11px] text-slate-500 mt-0.5 mb-1">
                      Enter the exact LLC, person, network name, or street address as shown in public records.
                    </span>
                    <input
                      ref={sideAInputRef}
                      id="reviewer-side-a"
                      name="target_value"
                      type="text"
                      required
                      autoFocus
                      autoCorrect="off"
                      autoCapitalize="none"
                      spellCheck={false}
                      value={sideAValue}
                      onChange={(e) => setSideAValue(e.target.value)}
                      onBlur={() => setTouchedFields((p) => ({ ...p, sideA: true }))}
                      aria-invalid={sideAInvalid ? 'true' : 'false'}
                      aria-describedby={sideAInvalid ? `${sideAHintId} ${sideAErrorId}` : sideAHintId}
                      placeholder="e.g., Acme Properties LLC or Menachem Gurevitch"
                      className={`reviewer-field w-full rounded-xl border px-3 py-2 text-base sm:text-xs text-slate-900 focus:outline-none focus:border-indigo-500 ${
                        sideAInvalid ? 'user-invalid-fallback border-rose-500 bg-rose-50/40' : 'border-slate-300 bg-white'
                      }`}
                    />
                    {sideAInvalid && (
                      <div id={sideAErrorId} className="mt-1 text-[11px] font-semibold text-rose-700">
                        Error: Enter at least 2 characters for the first network, entity, or person.
                      </div>
                    )}
                  </div>
                )}

                {/* Missed Connection Side B Fields */}
                {isMissed && (
                  <div className="space-y-3 rounded-xl border border-indigo-200/90 bg-indigo-50/40 p-3.5">
                    <fieldset>
                      <legend className="text-xs font-bold text-indigo-950 mb-1.5">
                        What missing item should link to this network? (Side B)
                      </legend>
                      <div className="grid grid-cols-2 gap-1.5">
                        {MISSING_TARGET_TYPES.map((opt) => (
                          <label
                            key={opt.value}
                            className={`flex items-center gap-2 rounded-lg border px-2.5 py-1.5 cursor-pointer text-xs ${
                              relatedTargetType === opt.value
                                ? 'border-indigo-400 bg-white text-indigo-950 font-bold shadow-2xs'
                                : 'border-indigo-100 bg-white/60 text-slate-700 hover:bg-white'
                            }`}
                          >
                            <input
                              type="radio"
                              name="related_target_type"
                              value={opt.value}
                              checked={relatedTargetType === opt.value}
                              onChange={() => setRelatedTargetType(opt.value)}
                              className="accent-indigo-600"
                            />
                            <span>{opt.label}</span>
                          </label>
                        ))}
                      </div>
                    </fieldset>

                    <div>
                      <label htmlFor="reviewer-side-b" className="block text-xs font-bold text-slate-800">
                        Missing {MISSING_TARGET_TYPES.find((t) => t.value === relatedTargetType)?.label || 'item'} name <span className="text-rose-600" aria-hidden="true">*</span>
                      </label>
                      <span id={sideBHintId} className="block text-[11px] text-slate-600 mt-0.5 mb-1">
                        Enter the exact name or street address of the entity, person, property, or network our builder missed.
                      </span>
                      <input
                        ref={sideBInputRef}
                        id="reviewer-side-b"
                        name="related_target_value"
                        type="text"
                        required
                        autoFocus={hasLockedSideA}
                        autoCorrect="off"
                        autoCapitalize="none"
                        spellCheck={false}
                        value={relatedTargetValue}
                        onChange={(e) => setRelatedTargetValue(e.target.value)}
                        onBlur={() => setTouchedFields((p) => ({ ...p, sideB: true }))}
                        aria-invalid={sideBInvalid ? 'true' : 'false'}
                        aria-describedby={sideBInvalid ? `${sideBHintId} ${sideBErrorId}` : sideBHintId}
                        placeholder={
                          relatedTargetType === 'property'
                            ? 'e.g., 142 Oakwood Ave, Hartford'
                            : relatedTargetType === 'principal'
                              ? 'e.g., Jane Doe'
                              : 'e.g., Acme Prop Mgmt Holdings LLC'
                        }
                        className={`reviewer-field w-full rounded-xl border px-3 py-2 text-base sm:text-xs text-slate-900 focus:outline-none focus:border-indigo-500 ${
                          sideBInvalid ? 'user-invalid-fallback border-rose-500 bg-rose-50/40' : 'border-slate-300 bg-white'
                        }`}
                      />
                      {sideBInvalid && (
                        <div id={sideBErrorId} className="mt-1 text-[11px] font-semibold text-rose-700">
                          Error: Enter the missing entity, person, property, or network name (at least 2 characters).
                        </div>
                      )}
                    </div>

                    <div>
                      <label htmlFor="reviewer-related-addr" className="block text-xs font-bold text-slate-700">
                        Known mailing address, HQ, or email clue for Side B (optional)
                      </label>
                      <span className="block text-[11px] text-slate-500 mt-0.5 mb-1">
                        Helps the nightly job spot address spelling variants, C/O prefixes, or over-blocked HQs.
                      </span>
                      <input
                        id="reviewer-related-addr"
                        name="related_address"
                        type="text"
                        autoCorrect="off"
                        autoCapitalize="none"
                        spellCheck={false}
                        value={relatedAddressClue}
                        onChange={(e) => setRelatedAddressClue(e.target.value)}
                        placeholder="e.g., C/O Acme Mgmt, 142 St Charles Ave, New Haven CT 06511"
                        className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-base sm:text-xs text-slate-900 focus:outline-none focus:border-indigo-500"
                      />
                    </div>
                  </div>
                )}

                <div>
                  <label htmlFor="reviewer-reason-select" className="block text-xs font-bold text-slate-700 mb-1">
                    {isMissed ? 'Why did the builder likely miss this connection?' : 'What looks wrong with this link?'}
                  </label>
                  <select
                    id="reviewer-reason-select"
                    name="reason_code"
                    autoFocus={!isMissed}
                    value={reasonCode}
                    onChange={(e) => setReasonCode(e.target.value)}
                    className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-base sm:text-xs font-medium text-slate-900 focus:border-blue-500 focus:outline-none"
                  >
                    {reasonOptions.map((opt) => (
                      <option key={opt.code} value={opt.code}>
                        {opt.label}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label htmlFor="reviewer-explanation" className="block text-xs font-bold text-slate-700">
                    {isMissed ? 'How are they connected in public records?' : 'Why do you believe this shouldn\'t be linked?'}{' '}
                    <span className="text-rose-600" aria-hidden="true">*</span>
                  </label>
                  {/* Hint placed ABOVE input per modern-web-guidance */}
                  <span id={explanationHintId} className="block text-[11px] text-slate-500 mt-0.5 mb-1">
                    {isMissed
                      ? 'Mention any shared HQ address variant, abbreviation, officer title suffix, C/O prefix, or filing clue that could fix the builder globally.'
                      : 'Mention any shared law office, CPA, UPS/PMB address, homonym name, or unrelated company clue that helps fix the system-wide logic.'}
                  </span>
                  <textarea
                    ref={explanationInputRef}
                    id="reviewer-explanation"
                    name="explanation"
                    required
                    minLength={6}
                    rows={3}
                    value={explanation}
                    onChange={(e) => setExplanation(e.target.value)}
                    onBlur={() => setTouchedFields((p) => ({ ...p, explanation: true }))}
                    aria-invalid={explanationInvalid ? 'true' : 'false'}
                    aria-describedby={explanationInvalid ? `${explanationHintId} ${explanationErrorId}` : explanationHintId}
                    placeholder={
                      isMissed
                        ? 'e.g., Both LLCs file at 142 St Charles Ave vs 142 Saint Charles Ave in 06511, or share principal Jane Doe (listed as Jane Doe MGRMEM on one filing)...'
                        : 'e.g., 129 Church St is a multi-tenant office building with dozens of unrelated attorneys and LLCs...'
                    }
                    className={`reviewer-field w-full rounded-xl border px-3 py-2 text-base sm:text-xs text-slate-900 focus:outline-none focus:border-blue-500 ${
                      explanationInvalid ? 'user-invalid-fallback border-rose-500 bg-rose-50/40' : 'border-slate-300 bg-white'
                    }`}
                  />
                  {explanationInvalid && (
                    <div id={explanationErrorId} className="mt-1 text-[11px] font-semibold text-rose-700">
                      Error: Please enter at least 6 characters explaining the evidence or linkage clue.
                    </div>
                  )}
                </div>

                <div className="space-y-3">
                  <div>
                    <label htmlFor="reviewer-evidence-url" className="block text-xs font-bold text-slate-700">
                      Public reference URL (optional)
                    </label>
                    <span id={urlHintId} className="block text-[11px] text-slate-500 mt-0.5 mb-1">
                      Format: https://example.com/...
                    </span>
                    <input
                      ref={urlInputRef}
                      id="reviewer-evidence-url"
                      name="evidence_url"
                      type="url"
                      autoCorrect="off"
                      autoCapitalize="none"
                      spellCheck={false}
                      value={evidenceUrl}
                      onChange={(e) => setEvidenceUrl(e.target.value)}
                      onBlur={() => setTouchedFields((p) => ({ ...p, evidenceUrl: true }))}
                      aria-invalid={urlInvalid ? 'true' : 'false'}
                      aria-describedby={urlInvalid ? `${urlHintId} ${urlErrorId}` : urlHintId}
                      placeholder="https://..."
                      className={`reviewer-field w-full rounded-xl border px-3 py-2 text-base sm:text-xs text-slate-900 focus:outline-none focus:border-blue-500 ${
                        urlInvalid ? 'user-invalid-fallback border-rose-500 bg-rose-50/40' : 'border-slate-300 bg-white'
                      }`}
                    />
                    {urlInvalid && (
                      <div id={urlErrorId} className="mt-1 text-[11px] font-semibold text-rose-700">
                        Error: Enter a valid URL starting with https:// or http://.
                      </div>
                    )}
                  </div>

                  <div>
                    <label htmlFor="reviewer-handle-input" className="block text-xs font-bold text-slate-700">
                      Your name / initials (optional)
                    </label>
                    <span className="block text-[11px] text-slate-500 mt-0.5 mb-1">
                      Saved locally so you only type it once.
                    </span>
                    <input
                      id="reviewer-handle-input"
                      name="reviewer_label"
                      type="text"
                      maxLength={80}
                      value={reviewerLabel}
                      onChange={(e) => setReviewerLabel(e.target.value)}
                      placeholder="e.g., Sam / CT Tenant Union"
                      className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-base sm:text-xs text-slate-900 focus:outline-none focus:border-blue-500"
                    />
                  </div>
                </div>

                <div className="flex items-start gap-2 rounded-xl border border-blue-100 bg-blue-50/70 px-3 py-2 text-[11px] text-blue-900">
                  <ShieldAlert size={14} aria-hidden="true" className="text-blue-600 shrink-0 mt-0.5" />
                  <span>
                    {isMissed ? (
                      <>
                        <strong>Organic builder tweaks, never hardcoded merges:</strong> Tonight&apos;s job checks both sides against loaded public records to find a generalized rule (address/abbreviation/role normalization, benign C/O prefix, or over-blocked HQ allowlist) that forms the link naturally across all 20 jurisdictions — and stays 1-click revertible from the daily digest.
                      </>
                    ) : (
                      <>
                        <strong>Evidence-checked nightly:</strong> Reports are never blindly applied. Tonight&apos;s job inspects the underlying public corporate &amp; tax filings, generalizes confirmed fixes across all 20 jurisdictions, rebuilds affected networks, and lets the maintainer 1-click revert from the daily digest.
                      </>
                    )}
                  </span>
                </div>

                {submitError && (
                  <div role="alert" className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-xs font-semibold text-rose-800">
                    {submitError}
                  </div>
                )}

                <div className="flex items-center justify-end gap-2 pt-1">
                  <button
                    type="button"
                    onClick={() => dialogRef.current?.close()}
                    className="rounded-xl border border-slate-300 bg-white px-4 py-2 text-xs font-bold text-slate-700 hover:bg-slate-50 cursor-pointer"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={submitting}
                    className={`inline-flex items-center gap-1.5 rounded-xl px-4 py-2 text-xs font-bold text-white disabled:opacity-60 cursor-pointer ${
                      isMissed ? 'bg-indigo-600 hover:bg-indigo-700' : 'bg-amber-600 hover:bg-amber-700'
                    }`}
                  >
                    {submitting ? (
                      <Loader2 size={14} aria-hidden="true" className="animate-spin" />
                    ) : isMissed ? (
                      <Link2 size={14} aria-hidden="true" />
                    ) : (
                      <Flag size={14} aria-hidden="true" />
                    )}
                    <span>{submitting ? 'Submitting…' : isMissed ? 'Submit Missed Connection' : 'Submit Report'}</span>
                  </button>
                </div>
              </form>
            )}
          </div>
        </div>
      )}
    </dialog>
  );
}
