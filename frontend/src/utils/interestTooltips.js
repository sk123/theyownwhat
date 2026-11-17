const ownedExpandedTriggers = new WeakSet();
let initialized = false;

function normalizeWhitespace(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function getElementTextWithImages(el) {
  if (!el) return '';
  let text = '';
  for (const node of el.childNodes) {
    if (node.nodeType === Node.TEXT_NODE) {
      text += node.nodeValue || '';
    } else if (node.nodeType === Node.ELEMENT_NODE) {
      if (node.getAttribute('aria-hidden') === 'true') continue;
      if (node.tagName === 'IMG') {
        text += ` ${node.getAttribute('alt') || ''} `;
      } else {
        text += ` ${getElementTextWithImages(node)} `;
      }
    }
  }
  return normalizeWhitespace(text);
}

function getAccessibleName(trigger, doc) {
  const labelledBy = trigger.getAttribute('aria-labelledby');
  if (labelledBy) {
    const parts = labelledBy
      .split(/\s+/)
      .filter(Boolean)
      .map((id) => doc.getElementById(id))
      .filter(Boolean)
      .map((el) => getElementTextWithImages(el));
    const joined = normalizeWhitespace(parts.join(' '));
    if (joined) return joined;
  }

  const ariaLabel = normalizeWhitespace(trigger.getAttribute('aria-label'));
  if (ariaLabel) return ariaLabel;

  const textContent = getElementTextWithImages(trigger);
  if (textContent) return textContent;

  return normalizeWhitespace(trigger.getAttribute('title'));
}

function isPopoverOpen(popoverEl) {
  try {
    if (popoverEl.matches(':popover-open')) return true;
  } catch {
    // Browser lacks :popover-open pseudo-class
  }
  return popoverEl.classList.contains(':popover-open');
}

export function syncInterestTooltips(root = typeof document !== 'undefined' ? document : null) {
  if (!root || typeof document === 'undefined') return;
  const doc = root.ownerDocument || document;
  const triggers = root.querySelectorAll ? root.querySelectorAll('[interestfor]') : [];

  triggers.forEach((trigger) => {
    const targetId = trigger.getAttribute('interestfor');
    if (!targetId) return;
    const hint = doc.getElementById(targetId);
    if (!hint || hint.getAttribute('popover') !== 'hint') return;

    const triggerName = getAccessibleName(trigger, doc);
    const hintText = getElementTextWithImages(hint);

    if (triggerName && hintText && triggerName === hintText) {
      return;
    }

    if (!hint.hasAttribute('role')) {
      hint.setAttribute('role', 'tooltip');
    }

    if (!triggerName) {
      if (!trigger.hasAttribute('aria-labelledby')) {
        trigger.setAttribute('aria-labelledby', targetId);
      }
      return;
    }

    const opensOnActivate =
      trigger.getAttribute('popovertarget') === targetId ||
      trigger.getAttribute('commandfor') === targetId;

    if (opensOnActivate) {
      if (!trigger.hasAttribute('aria-expanded') || ownedExpandedTriggers.has(trigger)) {
        trigger.setAttribute('aria-expanded', isPopoverOpen(hint) ? 'true' : 'false');
        ownedExpandedTriggers.add(trigger);
      }
    } else if (!trigger.hasAttribute('aria-describedby')) {
      trigger.setAttribute('aria-describedby', targetId);
    }
  });
}

function positionFallbackTooltip(popoverEl, activeSource) {
  if (
    typeof window === 'undefined' ||
    typeof CSS === 'undefined' ||
    CSS.supports('position-area', 'block-end') ||
    !(
      popoverEl.classList.contains('upgrade-prominent-tooltip') ||
      popoverEl.classList.contains('jdx-snapshot-tooltip')
    )
  ) {
    return;
  }

  const trigger =
    activeSource ||
    (popoverEl.id
      ? document.querySelector(`[interestfor="${CSS.escape(popoverEl.id)}"]`)
      : null);
  if (!trigger) return;

  const triggerRect = trigger.getBoundingClientRect();
  const popRect = popoverEl.getBoundingClientRect();
  const gap = 8;
  const margin = 12;

  let top = triggerRect.bottom + gap;
  if (top + popRect.height > window.innerHeight - margin && triggerRect.top - gap - popRect.height >= margin) {
    top = triggerRect.top - gap - popRect.height;
  }

  const preferredLeft = triggerRect.right - popRect.width;
  const maxLeft = Math.max(margin, window.innerWidth - popRect.width - margin);
  const left = Math.min(Math.max(margin, preferredLeft), maxLeft);

  popoverEl.style.top = `${Math.round(top)}px`;
  popoverEl.style.left = `${Math.round(left)}px`;
}

export function initInterestTooltips() {
  if (initialized || typeof window === 'undefined' || typeof document === 'undefined') {
    return;
  }
  initialized = true;

  const runInitialScan = () => syncInterestTooltips(document);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', runInitialScan, { once: true });
  } else {
    runInitialScan();
  }

  // Keep aria-expanded in sync from the popover's toggle event (capture phase; toggle does not bubble)
  document.addEventListener(
    'toggle',
    (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement) || !target.id) return;
      const isOpen = event.newState === 'open';

      if (isOpen) {
        positionFallbackTooltip(target, event.source);
      }

      const selector = `[interestfor="${CSS.escape(target.id)}"]`;
      document.querySelectorAll(selector).forEach((trigger) => {
        if (ownedExpandedTriggers.has(trigger)) {
          trigger.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
        }
      });
    },
    true
  );

  // Conditionally load Popover polyfill ONLY when native support is missing
  const hasNativePopover = Object.hasOwn(HTMLElement.prototype, 'popover');
  if (!hasNativePopover) {
    import('@oddbird/popover-polyfill');
  } else {
    // Where native popover exists without popover="hint", switch toggletips to "auto" in beforetoggle
    const probe = document.createElement('div');
    probe.setAttribute('popover', 'hint');
    if (probe.popover !== 'hint') {
      document.addEventListener(
        'beforetoggle',
        (event) => {
          const target = event.target;
          if (!(target instanceof HTMLElement) || !target.id) return;
          const isToggletip =
            target.getAttribute('popover') === 'hint' &&
            document.querySelector(`[popovertarget="${CSS.escape(target.id)}"]`);
          if (event.newState === 'open' && isToggletip) {
            target.setAttribute('popover', 'auto');
          }
        },
        true
      );
    }
  }

  // Conditionally load Interest Invokers polyfill ONLY when native support is missing
  if (
    typeof HTMLButtonElement !== 'undefined' &&
    !('interestForElement' in HTMLButtonElement.prototype)
  ) {
    import('interestfor');
  }
}
