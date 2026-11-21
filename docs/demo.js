const CLEAR = "\x1b[2J\x1b[H";

function parseCast(text) {
  const [header, ...events] = text.trim().split(/\r?\n/).map(line => JSON.parse(line));
  if (header.version !== 2 || events.length === 0 || events.some(
    ([time, kind, content], i) => !Number.isFinite(time) || time < 0 ||
      (i > 0 && time < events[i - 1][0]) || kind !== "o" || typeof content !== "string"
  )) throw new Error("Unsupported recording");
  return events;
}

function frameAt(events, seconds) {
  let frame = "";
  for (const [time, , text] of events) {
    if (time > seconds) break;
    frame += text;
    frame = frame.split(CLEAR).at(-1);
  }
  return frame.replaceAll("\r\n", "\n");
}

async function setup() {
  const trace = document.querySelector("#trace");
  const play = document.querySelector("#play");
  const restart = document.querySelector("#restart");
  const time = document.querySelector("#time");
  const progress = document.querySelector("#progress");
  const chapters = [...document.querySelectorAll("[data-start]")];
  try {
    const response = await fetch("./agent-nano.cast");
    if (!response.ok) throw new Error("Recording unavailable");
    const events = parseCast(await response.text());
    const duration = events.at(-1)[0];
    let position = 0;
    let animation = null;
    let started;

    function render() {
      const frame = frameAt(events, position);
      if (trace.textContent !== frame) trace.textContent = frame;
      progress.value = position;
      const seconds = Math.floor(position);
      time.textContent = `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")} / 1:30`;
      for (const chapter of chapters) {
        const start = Number(chapter.dataset.start);
        chapter.setAttribute("aria-pressed", String(start <= position && position < start + 30 || start === 60 && position === duration));
      }
    }
    function pause() {
      if (animation !== null) cancelAnimationFrame(animation);
      animation = null;
      play.textContent = position === duration ? "Replay" : "Play";
    }
    function tick(now) {
      position = Math.min(duration, (now - started) / 1000);
      render();
      if (position < duration) animation = requestAnimationFrame(tick);
      else pause();
    }
    function seek(seconds) {
      position = seconds;
      if (animation !== null) started = performance.now() - position * 1000;
      else play.textContent = "Play";
      render();
    }
    play.addEventListener("click", () => {
      if (animation !== null) return pause();
      if (position === duration) seek(0);
      started = performance.now() - position * 1000;
      play.textContent = "Pause";
      animation = requestAnimationFrame(tick);
    });
    restart.addEventListener("click", () => seek(0));
    chapters.forEach(chapter => chapter.addEventListener("click", () => seek(Number(chapter.dataset.start))));
    [play, restart, ...chapters].forEach(button => { button.disabled = false; });
    progress.max = duration;
    render();
  } catch {
    trace.textContent = "Could not load the recording. The transcript is available below.";
  }
}

if (typeof module !== "undefined") module.exports = { parseCast, frameAt };
if (typeof document !== "undefined") setup();
