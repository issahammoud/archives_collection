import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MantineProvider, createTheme } from '@mantine/core'
import { Notifications } from '@mantine/notifications'
import App from './App'
import '@mantine/core/styles.css'
import '@mantine/dates/styles.css'
import '@mantine/notifications/styles.css'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 60,
      retry: 1,
    },
  },
})

const theme = createTheme({
  primaryColor: 'red',
  colors: {
    red: [
      '#fff0f0',
      '#ffe0e0',
      '#ffb8b8',
      '#ff8d8d',
      '#ff6969',
      '#ff5757',
      '#ff4545',
      '#e63939',
      '#cc2d2d',
      '#b32020',
    ],
  },
  fontFamily: 'Inter, system-ui, -apple-system, sans-serif',
  headings: {
    fontFamily: 'Inter, system-ui, -apple-system, sans-serif',
  },
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <MantineProvider theme={theme}>
        <Notifications position="top-right" />
        <App />
      </MantineProvider>
    </QueryClientProvider>
  </StrictMode>
)
