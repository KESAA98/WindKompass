/* WindKompass – App Core
   - Section snap via wheel impulse
   - Allow inner scroll for selected sections
   - Icon nav: hover open (desktop) + tap toggle (mobile)
   - Load section HTML + optional section script
   - Background video playlist crossfade via manifest
*/

const state = {
  index: 0,
  locked: false,
  sections: [],
  sectionIds: [],
  innerScrollSections: new Set(),
  video: {
    list: [],
    idx: 0,
    fadeMs: 1200,
    fadeLeadMs: 1200,
    active: "A", // A or B
  }
};

const el = {
  stack: document.getElementById("sections"),
  nav: document.getElementById("icon-nav"),
  navToggle: document.getElementById("nav-toggle"),
  navMenu: document.getElementById("nav-menu"),
  bgA: document.getElementById("bgA"),
  bgB: document.getElementById("bgB"),
};

init().catch(err => console.error(err));

async function init(){
  // collect sections
  state.sections = Array.from(document.querySelectorAll(".section"));
  state.sectionIds = state.sections.map(s => s.id);

  state.sections.forEach(sec => {
    if (sec.dataset.innerScroll === "true") state.innerScrollSections.add(sec.id);
  });

  // Icon nav hover/tap behaviour
  setupNav();

  // Wheel snapping
  setupScrollSnap();

  // Load content html (and optional scripts)
  await loadAllSections();

  // Video playlist (via manifest)
  await setupVideoPlaylist();

  // First paint
  goTo(0, false);
}

function setupNav(){
  // Desktop hover open
  el.nav.addEventListener("mouseenter", () => {
    if (matchMedia("(hover:hover)").matches) openNav(true);
  });
  el.nav.addEventListener("mouseleave", () => {
    if (matchMedia("(hover:hover)").matches) openNav(false);
  });

  // Mobile tap toggle
  el.navToggle.addEventListener("click", () => {
    const isOpen = el.nav.classList.contains("is-open");
    openNav(!isOpen);
  });

  // Menu click → goTo section
  el.navMenu.querySelectorAll("button[data-target]").forEach(btn => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.target;
      const i = state.sectionIds.indexOf(target);
      if (i >= 0) goTo(i);
      openNav(false);
    });
  });

  // close on ESC
  window.addEventListener("keydown", (e) => {
    if (e.key === "Escape") openNav(false);
  });
}

function openNav(open){
  el.nav.classList.toggle("is-open", !!open);
  el.navToggle.setAttribute("aria-expanded", open ? "true" : "false");
}

function setupScrollSnap(){
  // prevent body scroll is already in CSS; we use wheel to snap
  window.addEventListener("wheel", onWheel, { passive:false });
  window.addEventListener("resize", () => goTo(state.index, false));
}

function onWheel(e){
  // If wheel happens inside an inner-scroll container and it can scroll, let it scroll.
  const inner = findInnerScrollable(e);
  if (inner && canInnerConsume(inner, e.deltaY)) return;

  e.preventDefault();
  if (state.locked) return;

  state.locked = true;
  const dir = Math.sign(e.deltaY);
  if (dir > 0) goTo(state.index + 1);
  if (dir < 0) goTo(state.index - 1);

  setTimeout(() => (state.locked = false), 520);
}

function findInnerScrollable(e){
  const path = e.composedPath ? e.composedPath() : [];
  // our inner container is the only scroll area
  return path.find(n => n && n.classList && n.classList.contains("inner-container") && n.scrollHeight > n.clientHeight);
}

function canInnerConsume(inner, deltaY){
  const atTop = inner.scrollTop <= 0;
  const atBottom = inner.scrollTop + inner.clientHeight >= inner.scrollHeight - 1;
  if (deltaY > 0 && !atBottom) return true;
  if (deltaY < 0 && !atTop) return true;
  return false;
}

function goTo(i, animate=true){
  const max = state.sections.length - 1;
  const next = Math.max(0, Math.min(max, i));
  state.index = next;

  // snap transform
  if (!animate) el.stack.style.transition = "none";
  el.stack.style.transform = `translateY(${-next * window.innerHeight}px)`;
  if (!animate) {
    // force reflow then restore transition
    void el.stack.offsetHeight;
    el.stack.style.transition = "";
  }

  // nav active highlight
  el.navMenu.querySelectorAll("button[data-target]").forEach(b => {
    b.classList.toggle("active", b.dataset.target === state.sectionIds[next]);
  });
}

/* -------------------------
   SECTION LOADING
-------------------------- */

async function loadAllSections(){
  const inners = Array.from(document.querySelectorAll(".inner-container[data-load]"));
  for (const inner of inners){
    const url = inner.dataset.load;
    await loadHtmlInto(inner, url);
    // Optional: auto-load a script if declared in content via: <div data-section-script="..."></div>
    await loadOptionalSectionScript(inner);
  }
}

async function loadHtmlInto(container, url){
  try{
    const res = await fetch(url, { cache: "no-cache" });
    if(!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
    const html = await res.text();
    container.innerHTML = html;
  }catch(err){
    container.innerHTML = `<div style="padding:10px">
      <strong>Fehler beim Laden:</strong><br>${escapeHtml(url)}
    </div>`;
    console.error(err);
  }
}

// If content.html includes an element like:
// <div data-section-script="sections/overview/section.js"></div>
async function loadOptionalSectionScript(container){
  const marker = container.querySelector("[data-section-script]");
  if(!marker) return;

  const src = marker.getAttribute("data-section-script");
  if(!src) return;

  // avoid double-load
  if (document.querySelector(`script[data-dynamic="true"][src="${src}"]`)) return;

  await new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = src;
    s.async = true;
    s.dataset.dynamic = "true";
    s.onload = () => resolve();
    s.onerror = () => reject(new Error(`Script load failed: ${src}`));
    document.body.appendChild(s);
  }).catch(console.error);
}

function escapeHtml(str){
  return String(str).replace(/[&<>"']/g, s => ({
    "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"
  }[s]));
}

/* -------------------------
   VIDEO PLAYLIST
-------------------------- */

async function setupVideoPlaylist(){
  // expects: /assets/videos/manifest.json
  // { "fadeMs":1200, "fadeLeadMs":1200, "videos":[ "bg_01.mp4", "bg_02.mp4" ] }
  try{
    const res = await fetch("assets/videos/manifest.json", { cache:"no-cache" });
    if(!res.ok) throw new Error("No videos manifest");
    const json = await res.json();

    state.video.fadeMs = typeof json.fadeMs === "number" ? json.fadeMs : state.video.fadeMs;
    state.video.fadeLeadMs = typeof json.fadeLeadMs === "number" ? json.fadeLeadMs : state.video.fadeLeadMs;

    state.video.list = (json.videos || []).map(v => `assets/videos/${v}`);
    if(state.video.list.length === 0) throw new Error("Empty video list");

    // sync CSS fade timing
    document.documentElement.style.setProperty("--fade-ms", `${state.video.fadeMs}ms`);

    // Start: load first into A and show
    await setVideoSource(el.bgA, state.video.list[0]);
    el.bgA.classList.add("is-visible");
    el.bgB.classList.remove("is-visible");
    state.video.active = "A";

    // when A is near end, crossfade to next
    attachFadeScheduler(el.bgA);

  }catch(err){
    console.warn("Video playlist disabled:", err);
  }
}

function attachFadeScheduler(videoEl){
  // Ensure we schedule based on duration once metadata is ready
  const onReady = () => {
    scheduleFade(videoEl);
  };

  videoEl.removeEventListener("loadedmetadata", onReady);
  videoEl.addEventListener("loadedmetadata", onReady, { once:true });

  // If already has metadata
  if (videoEl.duration && isFinite(videoEl.duration)) {
    scheduleFade(videoEl);
  }

  // reschedule if playback restarts
  videoEl.addEventListener("ended", () => scheduleFade(videoEl));
}

function scheduleFade(currentEl){
  const dur = currentEl.duration;
  if(!dur || !isFinite(dur)) return;

  const lead = state.video.fadeLeadMs / 1000;
  const t = Math.max(0, dur - lead);

  // clear previous timer
  if (currentEl._fadeTimer) clearTimeout(currentEl._fadeTimer);

  currentEl._fadeTimer = setTimeout(() => {
    crossfadeToNext();
  }, t * 1000);
}

async function crossfadeToNext(){
  if(state.video.list.length < 2) return;

  // pick next
  state.video.idx = (state.video.idx + 1) % state.video.list.length;
  const nextSrc = state.video.list[state.video.idx];

  const from = state.video.active === "A" ? el.bgA : el.bgB;
  const to   = state.video.active === "A" ? el.bgB : el.bgA;

  await setVideoSource(to, nextSrc);

  // show "to", hide "from"
  to.classList.add("is-visible");
  from.classList.remove("is-visible");

  state.video.active = (state.video.active === "A") ? "B" : "A";

  // schedule next fade on the newly active element
  attachFadeScheduler(to);
}

async function setVideoSource(videoEl, src){
  return new Promise((resolve) => {
    // stop old
    try { videoEl.pause(); } catch {}
    videoEl.removeAttribute("src");
    videoEl.load();

    videoEl.src = src;

    const onCanPlay = async () => {
      videoEl.removeEventListener("canplay", onCanPlay);
      try { await videoEl.play(); } catch {}
      resolve();
    };

    videoEl.addEventListener("canplay", onCanPlay, { once:true });
    videoEl.load();
  });
}
