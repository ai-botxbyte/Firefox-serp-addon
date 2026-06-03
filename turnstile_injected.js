// Turnstile Auto-Click: MAIN world script
// Detects Cloudflare Turnstile checkbox inside Shadow DOM and reports its position
if (window.top !== window.self && window.location.href.includes('challenges.cloudflare.com')) {
    window.dtp = 1
    function getRandomInt(min, max) {
        return Math.floor(Math.random() * (max - min + 1)) + min;
    }

    let screenX = getRandomInt(800, 1200);
    let screenY = getRandomInt(400, 600);

    Object.defineProperty(MouseEvent.prototype, 'screenX', { value: screenX });
    Object.defineProperty(MouseEvent.prototype, 'screenY', { value: screenY });
    function runInjectionLogic() {
        function getNativeAttachShadow() {
            try {
                const iframe = document.createElement('iframe');
                iframe.style.display = 'none';
                document.body.appendChild(iframe);
                const nativeAttachShadow = iframe.contentWindow.Element.prototype.attachShadow;
                document.body.removeChild(iframe);
                return nativeAttachShadow;
            } catch (e) {
                console.error("Failed to create iframe for native function extraction:", e);
                return null;
            }
        }

        // --- Stealthy hook code (spy mode) ---
        try {
            const originalAttachShadow = getNativeAttachShadow();
            if (!originalAttachShadow) {
                console.error("Aborting: Could not retrieve native attachShadow.");
                return;
            }

            Element.prototype.attachShadow = function(...args) {
                const shadowRoot = originalAttachShadow.apply(this, args);
                if (shadowRoot) {
                    const existingCheckbox = shadowRoot.querySelector('input[type="checkbox"]');
                    if (existingCheckbox) {
                        window.mySecretCheckbox = existingCheckbox;
                    } else {
                        const observer = new MutationObserver((mutations, obs) => {
                            const checkbox = shadowRoot.querySelector('input[type="checkbox"]');
                            if (checkbox) {
                                window.mySecretCheckbox = checkbox;
                                obs.disconnect();
                            }
                        });
                        observer.observe(shadowRoot, {
                            childList: true,
                            subtree: true
                        });
                    }
                }
                return shadowRoot;
            };
        } catch (e) {
            console.error("Error during prototype override:", e);
        }
    }

    if (document.body) {
        runInjectionLogic();
    } else {
        const observer = new MutationObserver(() => {
            if (document.body) {
                runInjectionLogic();
                observer.disconnect();
            }
        });

        observer.observe(document.documentElement, {
            childList: true
        });
    }

    /**
    * Poll for the element; if found, compute its center position ratios and send them as a message.
     */
    (function pollAndReportCenterRatio() {
        if (window.mySecretCheckbox) {
            const checkbox = window.mySecretCheckbox;
            const rect = checkbox.getBoundingClientRect();
            const windowWidth = window.innerWidth;
            const windowHeight = window.innerHeight;

            if (windowWidth > 0 && windowHeight > 0) {
                const centerX = rect.left + rect.width / 2;
                const centerY = rect.top + rect.height / 2;
                const xRatio = centerX / windowWidth;
                const yRatio = centerY / windowHeight;

                // Key step: broadcast the message from the MAIN world
                window.postMessage({
                    type: 'CHECKBOX_POSITION_RATIO',
                    payload: {
                        xRatio: xRatio,
                        yRatio: yRatio
                    }
                }, '*');
            }
            try { delete window.mySecretCheckbox; } catch (e) {}
        } else {
            setTimeout(pollAndReportCenterRatio, 200);
        }
    })();
}
