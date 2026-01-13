import { useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AppShell, Box, Text } from '@mantine/core'
import Header from './components/Header'
import Sidebar from './components/Sidebar'
import MainContent from './components/MainContent'
import SidebarToggle from './components/SidebarToggle'
import { useStore } from './store'
import { articlesApi } from './api'

function App() {
  const { setFilters, drawerOpen } = useStore()

  const { data: dateRange } = useQuery({
    queryKey: ['dateRange'],
    queryFn: articlesApi.getDateRange,
  })

  useEffect(() => {
    if (dateRange?.min_date && dateRange?.max_date) {
      setFilters({
        dateStart: dateRange.min_date,
        dateEnd: dateRange.max_date,
      })
    }
  }, [dateRange, setFilters])

  return (
    <AppShell header={{ height: 100 }} padding={0}>
      <AppShell.Header withBorder>
        <Header />
      </AppShell.Header>

      <AppShell.Main>
        <Box style={{ display: 'flex', minHeight: 'calc(100vh - 100px)', position: 'relative' }}>
          <Box
            style={{
              width: drawerOpen ? '15%' : 0,
              minWidth: drawerOpen ? 200 : 0,
              maxWidth: drawerOpen ? 280 : 0,
              backgroundColor: 'white',
              padding: drawerOpen ? 'var(--mantine-spacing-md)' : 0,
              overflowY: 'auto',
              overflowX: 'hidden',
              transition: 'all 300ms ease',
              boxShadow: drawerOpen ? '2px 0 8px rgba(0,0,0,0.05)' : 'none',
            }}
          >
            {drawerOpen && <Sidebar />}
          </Box>

          <SidebarToggle />

          <Box
            style={{
              flex: 1,
              padding: 'var(--mantine-spacing-xl)',
              transition: 'all 300ms ease',
            }}
          >
            <MainContent />
          </Box>
        </Box>

        <Box
          component="footer"
          py="md"
          px="xl"
        >
          <Text ta="center" size="sm">
            &copy; Copyright Intuitive Deep Learning {new Date().getFullYear()}.
          </Text>
        </Box>
      </AppShell.Main>
    </AppShell>
  )
}

export default App
