(function () {
  var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  /* Полоса прогресса на каждый scroll + style.width дает лишние перерисовки на телефонах */
  var liteTouch =
    window.matchMedia("(max-width: 992px)").matches ||
    window.matchMedia("(hover: none) and (pointer: coarse)").matches;

  if (!reduceMotion && !liteTouch) {
    var progressEl = document.createElement("div");
    progressEl.className = "scroll-progress";
    progressEl.setAttribute("aria-hidden", "true");
    var fill = document.createElement("span");
    fill.className = "scroll-progress__fill";
    progressEl.appendChild(fill);
    document.body.insertBefore(progressEl, document.body.firstChild);

    var ticking = false;
    function updateScrollProgress() {
      var el = document.documentElement;
      var scrollTop = el.scrollTop || document.body.scrollTop;
      var height = el.scrollHeight - el.clientHeight;
      var p = height > 0 ? Math.min(100, Math.max(0, (scrollTop / height) * 100)) : 0;
      fill.style.width = p + "%";
      ticking = false;
    }

    function onScroll() {
      if (!ticking) {
        ticking = true;
        requestAnimationFrame(updateScrollProgress);
      }
    }

    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", updateScrollProgress);
    window.addEventListener("load", updateScrollProgress);
    updateScrollProgress();
  }

  var selectors = ".reveal-up, .reveal-left, .reveal-right, .reveal-zoom, .reveal-flip";

  function revealAll() {
    document.querySelectorAll(selectors).forEach(function (el) {
      el.classList.add("is-inview");
    });
  }

  if (!reduceMotion && "IntersectionObserver" in window) {
    var els = document.querySelectorAll(selectors);
    if (els.length) {
      try {
        var io = new IntersectionObserver(
          function (entries) {
            entries.forEach(function (entry) {
              if (entry.isIntersecting) {
                entry.target.classList.add("is-inview");
                io.unobserve(entry.target);
              }
            });
          },
          {
            threshold: 0.06,
            rootMargin: "0px 0px -40px 0px"
          }
        );
        els.forEach(function (el) {
          io.observe(el);
        });
      } catch (err) {
        revealAll();
      }
    }
  } else {
    revealAll();
  }

  var pollForm = document.getElementById("poll-form");
  if (pollForm) {
    pollForm.addEventListener("submit", function (e) {
      e.preventDefault();
      var res = document.getElementById("poll-result");
      var sub = pollForm.querySelector(".poll-submit");
      var fd = new FormData(pollForm);
      var listen = fd.get("listen");
      if (!res || !listen) return;

      res.classList.remove("poll-result--success", "poll-result--error", "is-visible");
      if (typeof res.offsetWidth === "number") {
        void res.offsetWidth;
      }

      if (listen === "yes") {
        res.classList.add("poll-result--success");
        res.textContent = "Правильно.";
        res.classList.add("is-visible");
        if (sub) sub.disabled = true;
      } else {
        res.classList.add("poll-result--error");
        res.textContent = "Произошла ошибка попробуйте снова.";
        res.classList.add("is-visible");
        if (sub) sub.disabled = false;
      }
    });
  }
})();
