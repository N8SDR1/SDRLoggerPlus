import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClientProvider } from '@tanstack/react-query'
import { ModuleRegistry, AllCommunityModule } from 'ag-grid-community'
import App from './App'
import { queryClient } from './api/queryClient'
import './index.css'

// Register AG Grid modules
ModuleRegistry.registerModules([AllCommunityModule])

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </React.StrictMode>,
)
