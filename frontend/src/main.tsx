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
  primaryColor: 'inky-navy',
  colors: {
    'inky-navy': [
      '#e8eaf1',
      '#c4cce0',
      '#9faecf',
      '#7a91bf',
      '#5574af',
      '#30579f',
      '#1B2A5E', // Inky Navy
      '#16234f',
      '#111c40',
      '#0c1431',
    ],
    'inky-red': [
      '#ffebeb',
      '#ffc2c2',
      '#ff9999',
      '#ff7070',
      '#FF4A4A', // Inky Red
      '#f03d3d',
      '#d62a2a',
      '#bc1717',
      '#a20404',
      '#880000',
    ],
    'inky-dark': [
      '#e7e8ec',
      '#c2c5d6',
      '#9da2be',
      '#787fa6',
      '#535c8e',
      '#2e3976',
      '#0F1941', // Inky Dark
      '#0d163a',
      '#0b1333',
      '#09102c',
    ],
  },
  fontFamily: 'Poppins, sans-serif',
  headings: {
    fontFamily: 'Poppins, sans-serif',
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
