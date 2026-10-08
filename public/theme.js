(function () {
  var KEY = 'theme';

  function stored() {
    try {
      var t = localStorage.getItem(KEY);
      if (t === 'light' || t === 'dark') return t;
    } catch (e) {}
    return null; // no preference -> follow system
  }

  function resolved() {
    var p = stored();
    if (p === 'light' || p === 'dark') return p;
    try {
      return window.matchMedia('(prefers-color-scheme: dark)').matches
        ? 'dark' : 'light';
    } catch (e) {
      return 'light';
    }
  }

  function apply() {
    var r = resolved();
    document.documentElement.setAttribute('data-theme', r);
    var btn = document.getElementById('theme-toggle');
    if (btn) btn.textContent = r === 'dark' ? 'Dark' : 'Light';
  }

  // Set before first paint to avoid a flash.
  apply();

  document.addEventListener('DOMContentLoaded', function () {
    var btn = document.getElementById('theme-toggle');
    if (!btn) return;
    btn.addEventListener('click', function () {
      var next = resolved() === 'dark' ? 'light' : 'dark';
      try { localStorage.setItem(KEY, next); } catch (e) {}
      apply();
    });
    apply();
  });
})();
