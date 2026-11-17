<script setup lang="ts">
/** Hero action: copies a ready-made install prompt for a coding agent. */
import { ref } from 'vue'
import { withBase } from 'vitepress'

const prompt = [
  'Install bamboo-raft for me: self-hosted VMs for AI agents.',
  'Follow https://nathabonfim59.github.io/bamboo-raft/agent-install.md step by step.',
  'Ask me before you run anything on a remote host.',
].join('\n')

const copied = ref(false)
let timer: ReturnType<typeof setTimeout> | undefined

async function copy() {
  try {
    await navigator.clipboard.writeText(prompt)
  } catch {
    // No clipboard (insecure context, denied): open the page with the prompt instead.
    window.location.href = withBase('/agent-install')
    return
  }
  copied.value = true
  clearTimeout(timer)
  timer = setTimeout(() => (copied.value = false), 2000)
}
</script>

<template>
  <div class="bb-agent">
    <button type="button" class="bb-agent-btn" :title="prompt" @click="copy">
      <span aria-hidden="true">&gt;_</span>
      {{ copied ? 'Copied: paste it into your agent' : 'Copy prompt for your agent' }}
    </button>
    <a class="bb-agent-link" :href="withBase('/agent-install')">What it does</a>
    <span class="sr-only" aria-live="polite">{{ copied ? 'Prompt copied' : '' }}</span>
  </div>
</template>

<style scoped>
.bb-agent {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px 16px;
  margin-top: 16px;
}
.bb-agent-btn {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  padding: 0 18px;
  min-height: 40px;
  border: 1px dashed var(--vp-button-alt-border);
  font-family: var(--bb-mono);
  font-size: 13px;
  color: var(--vp-c-text-1);
  background: transparent;
  cursor: pointer;
  transition: border-color 0.2s, color 0.2s;
}
.bb-agent-btn span { color: var(--bb-accent); font-weight: 700; }
.bb-agent-btn:hover { border-color: var(--bb-accent); border-style: solid; color: var(--bb-accent); }
.bb-agent-link {
  font-size: 13px;
  color: var(--vp-c-text-2);
  text-decoration: underline;
  text-underline-offset: 3px;
}
.bb-agent-link:hover { color: var(--bb-accent); }
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  overflow: hidden;
  clip: rect(0 0 0 0);
  white-space: nowrap;
}
</style>
