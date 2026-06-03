// CSP Meta Tag Stripper — MAIN world, document_start
// Removes <meta http-equiv="Content-Security-Policy"> tags so that
// evaluate actions using eval()/new Function() work on strict-CSP pages.
(function () {
  'use strict';

  function removeCSPMeta(node) {
    if (
      node.tagName === 'META' &&
      node.httpEquiv &&
      node.httpEquiv.toLowerCase() === 'content-security-policy'
    ) {
      node.remove();
      return true;
    }
    return false;
  }

  // Sweep any meta tags already present (edge case: inline HTML parsed before us)
  function sweepExisting() {
    document.querySelectorAll('meta[http-equiv]').forEach(removeCSPMeta);
  }

  if (document.head) sweepExisting();

  // Watch for dynamically added CSP meta tags
  var observer = new MutationObserver(function (mutations) {
    for (var i = 0; i < mutations.length; i++) {
      var added = mutations[i].addedNodes;
      for (var j = 0; j < added.length; j++) {
        var node = added[j];
        if (node.nodeType !== Node.ELEMENT_NODE) continue;
        removeCSPMeta(node);
        if (node.querySelectorAll) {
          node.querySelectorAll('meta[http-equiv]').forEach(removeCSPMeta);
        }
      }
    }
  });

  observer.observe(document.documentElement, {
    childList: true,
    subtree: true,
  });
})();
