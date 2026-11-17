<script setup lang="ts">
/**
 * Home page demo: two sandboxes on one time axis, "time until the tests run". A playhead sweeps
 * the axis at 60x; the run from scratch spends minutes in setup.sh, the run from a snapshot is
 * done in under a minute. Below it, the three commands that make the second run possible. The
 * project and its timings are illustrative, and the page says so.
 *
 * Plays and loops only while on screen and the tab is visible. With reduced motion it shows the
 * finished timeline.
 */
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'

const SPEED = 60 // demo seconds per wall-clock second
const AXIS = 600 // seconds the axis spans (10:00)
const HOLD_MS = 3000 // the finished timeline stays before the loop restarts

type Kind = 'boot' | 'setup' | 'test'
interface Seg { label: string; sec: number; kind: Kind }
interface Run { name: string; cmd: string; segs: Seg[] }

const runs: Run[] = [
  {
    name: 'From scratch',
    cmd: 'bbraft new  →  ./setup.sh  →  make test',
    segs: [
      { label: 'boot', sec: 19, kind: 'boot' },
      { label: 'apt', sec: 95, kind: 'setup' },
      { label: 'go mod', sec: 70, kind: 'setup' },
      { label: 'npm ci', sec: 140, kind: 'setup' },
      { label: 'playwright', sec: 110, kind: 'setup' },
      { label: 'build', sec: 83, kind: 'setup' },
      { label: 'test', sec: 41, kind: 'test' },
    ],
  },
  {
    name: 'From a snapshot',
    cmd: 'bbraft new --from ready-to-test  →  make test',
    segs: [
      { label: 'boot', sec: 7, kind: 'boot' },
      { label: 'test', sec: 41, kind: 'test' },
    ],
  },
]

const totals = runs.map((r) => r.segs.reduce((n, s) => n + s.sec, 0))
const loopMs = (Math.max(...totals) / SPEED) * 1000 + HOLD_MS

const steps = [
  { n: '1', title: 'Set it up once', cmd: 'bbraft ssh <id> -- ./setup.sh' },
  { n: '2', title: 'Save the disk', cmd: 'bbraft snapshot <id> ready-to-test' },
  { n: '3', title: 'Start from it', cmd: 'bbraft new --from ready-to-test' },
]

const ms = ref(loopMs - 1) // server render and reduced motion: finished
const now = computed(() => Math.min((ms.value / 1000) * SPEED, Math.max(...totals)))
const pct = (sec: number) => `${(sec / AXIS) * 100}%`

function clock(seconds: number): string {
  const s = Math.floor(seconds)
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`
}

const root = ref<HTMLElement | null>(null)
let onScreen = true
let frame = 0
let last = 0
let observer: IntersectionObserver | undefined

function tick(t: number) {
  const dt = last ? Math.min(t - last, 100) : 0
  last = t
  if (onScreen && !document.hidden) ms.value = (ms.value + dt) % loopMs
  frame = requestAnimationFrame(tick)
}

onMounted(() => {
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return
  ms.value = 0
  observer = new IntersectionObserver(([e]) => (onScreen = e.isIntersecting), { threshold: 0.2 })
  if (root.value) observer.observe(root.value)
  frame = requestAnimationFrame(tick)
})

onBeforeUnmount(() => {
  cancelAnimationFrame(frame)
  observer?.disconnect()
})

const summary =
  `Time until the tests run. From scratch: ${clock(totals[0])}, most of it in setup. ` +
  `From a snapshot: ${clock(totals[1])}. Set it up once, save the disk with bbraft snapshot, ` +
  'and start every next sandbox from it with bbraft new --from.'
</script>

<template>
  <section ref="root" class="bb-tl" :aria-label="summary">
    <div class="bb-tl-inner">
      <div class="bb-card" aria-hidden="true">
        <div class="bb-card-head">
          <span class="bb-eyebrow">Time until the tests run</span>
          <span class="bb-clock">{{ clock(now) }}</span>
        </div>

        <div v-for="(run, i) in runs" :key="run.name" class="bb-run" :class="{ 'is-fast': i === 1 }">
          <div class="bb-run-meta">
            <span class="bb-run-name">{{ run.name }}</span>
            <code class="bb-run-cmd">{{ run.cmd }}</code>
          </div>
          <div class="bb-track">
            <div class="bb-bar" :style="{ clipPath: `inset(0 calc(100% - ${pct(now)}) 0 0)` }">
              <span
                v-for="seg in run.segs" :key="seg.label" class="bb-seg" :class="`is-${seg.kind}`"
                :style="{ width: pct(seg.sec) }"
              ><span v-if="seg.sec / AXIS >= 0.1" class="bb-seg-label">{{ seg.label }}</span></span>
            </div>
            <span
              class="bb-result" :class="{ 'is-shown': now >= totals[i] }"
              :style="{ left: pct(totals[i]) }"
            >{{ i === 1 ? '✓ ' : '' }}{{ clock(totals[i]) }}</span>
          </div>
        </div>

        <div class="bb-axis">
          <span v-for="m in [0, 2, 4, 6, 8, 10]" :key="m" :style="{ left: pct(m * 60) }">{{ m }}m</span>
          <i class="bb-playhead" :style="{ left: pct(now) }" />
        </div>
      </div>

      <ol class="bb-steps">
        <li v-for="s in steps" :key="s.n">
          <span class="bb-step-n">{{ s.n }}</span>
          <span class="bb-step-title">{{ s.title }}</span>
          <code>{{ s.cmd }}</code>
        </li>
      </ol>
      <p class="bb-fine">Illustrative project and timings, played at 60&times;.</p>
    </div>
  </section>
</template>

<style scoped>
.bb-tl { padding: 0 24px; margin: 8px 0 56px; }
@media (min-width: 640px) { .bb-tl { padding: 0 48px; } }
@media (min-width: 960px) { .bb-tl { padding: 0 64px; margin-bottom: 72px; } }
.bb-tl-inner { max-width: 1152px; margin: 0 auto; }

.bb-card {
  --setup: #c9892c;
  --setup-2: #b07824;
  --boot: var(--vp-c-text-3);
  border: 1px solid var(--vp-c-divider);
  background: var(--vp-c-bg-soft);
  padding: 20px 24px 12px;
}
.dark .bb-card { --setup: #d9a24a; --setup-2: #c18d3a; }

.bb-card-head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  margin-bottom: 18px;
}
.bb-eyebrow {
  font-family: var(--bb-mono);
  font-size: 11px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--vp-c-text-2);
}
.bb-clock {
  font-family: var(--bb-mono);
  font-size: 22px;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
  color: var(--vp-c-text-1);
}

.bb-run { margin-bottom: 18px; }
.bb-run-meta {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 4px 12px;
  margin-bottom: 8px;
}
.bb-run-name { font-weight: 600; font-size: 14px; color: var(--vp-c-text-1); }
.bb-run.is-fast .bb-run-name { color: var(--bb-accent); }
.bb-run-cmd {
  font-family: var(--bb-mono);
  font-size: 12px;
  color: var(--vp-c-text-2);
  background: none;
  padding: 0;
}

.bb-track { position: relative; height: 28px; margin-right: 52px; }
.bb-bar { display: flex; height: 100%; }
.bb-seg {
  height: 100%;
  flex: none;
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
  border-right: 1px solid var(--vp-c-bg-soft);
}
.bb-seg.is-boot { background: var(--boot); }
.bb-seg.is-setup { background: var(--setup); }
.bb-seg.is-setup:nth-child(odd) { background: var(--setup-2); }
.bb-seg.is-test { background: var(--bb-accent); }
.bb-seg-label {
  font-family: var(--bb-mono);
  font-size: 11px;
  color: #1a1206;
  white-space: nowrap;
}

.bb-result {
  position: absolute;
  top: 50%;
  transform: translate(8px, -50%);
  font-family: var(--bb-mono);
  font-size: 13px;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
  color: var(--vp-c-text-2);
  opacity: 0;
  transition: opacity 0.2s;
}
.bb-result.is-shown { opacity: 1; }
.bb-run.is-fast .bb-result { color: var(--bb-accent); }

.bb-axis {
  position: relative;
  height: 22px;
  margin-right: 52px;
  border-top: 1px solid var(--vp-c-divider);
  font-family: var(--bb-mono);
  font-size: 10px;
  color: var(--vp-c-text-3);
}
.bb-axis span { position: absolute; top: 4px; transform: translateX(-50%); }
.bb-axis span:first-child { transform: none; }
.bb-playhead {
  position: absolute;
  bottom: 22px;
  width: 2px;
  height: 140px;
  margin-left: -1px;
  background: var(--vp-c-text-1);
  opacity: 0.35;
  pointer-events: none;
}

.bb-steps {
  list-style: none;
  margin: 16px 0 0;
  padding: 0;
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
}
@media (max-width: 767px) { .bb-steps { grid-template-columns: 1fr; } }
.bb-steps li {
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 4px 10px;
  align-items: baseline;
  padding: 12px 14px;
  border: 1px solid var(--vp-c-divider);
  min-width: 0;
}
.bb-step-n { font-family: var(--bb-mono); font-weight: 700; color: var(--bb-accent); }
.bb-step-title { font-weight: 600; font-size: 14px; }
.bb-steps code {
  grid-column: 2;
  font-family: var(--bb-mono);
  font-size: 12px;
  color: var(--vp-code-color);
  background: none;
  padding: 0;
  overflow-wrap: anywhere;
}
.bb-fine {
  margin: 10px 0 0;
  font-family: var(--bb-mono);
  font-size: 11px;
  color: var(--vp-c-text-3);
}

@media (max-width: 639px) {
  .bb-card { padding: 16px 16px 8px; }
  .bb-clock { font-size: 19px; }
  .bb-track, .bb-axis { margin-right: 44px; }
  .bb-seg-label { display: none; }
  .bb-run-cmd { font-size: 11px; }
}
</style>
