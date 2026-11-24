import { createRouter, createWebHistory } from 'vue-router'

import FieldDeactivationView from '@/views/FieldDeactivationView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/field-deactivation', name: 'field-deactivation', component: FieldDeactivationView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
