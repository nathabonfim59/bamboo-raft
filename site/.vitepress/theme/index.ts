import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import { h } from 'vue'
import '@fontsource-variable/inter'
import '@fontsource-variable/jetbrains-mono'
import './style.css'
import TerminalDemo from './TerminalDemo.vue'
import AgentPrompt from './AgentPrompt.vue'

export default {
  extends: DefaultTheme,
  // The terminal demo sits between the hero and the feature cards on the home page.
  Layout: () => h(DefaultTheme.Layout, null, {
      'home-hero-actions-after': () => h(AgentPrompt),
      'home-features-before': () => h(TerminalDemo),
    }),
} satisfies Theme
