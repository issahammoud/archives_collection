import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AppShell, Box, ActionIcon } from '@mantine/core'
import { IconChevronRight } from '@tabler/icons-react'
import Header from './components/Header'
import Sidebar from './components/Sidebar'
import MainContent from './components/MainContent'
import { useStore } from './store'
import { articlesApi } from './api'

function App() {
  const { setFilters } = useStore()
  const [sidebarOpen, setSidebarOpen] = useState(false)

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
    <AppShell header={{ height: 80 }} padding="md">
      <AppShell.Header withBorder>
        <Header />
      </AppShell.Header>

      <AppShell.Main bg="#f9fafb">
        <Box style={{ position: 'relative' }}>
          <Box
            onMouseEnter={() => setSidebarOpen(true)}
            onMouseLeave={() => setSidebarOpen(false)}
            style={{
              position: 'absolute',
              top: 0,
              left: -16,
              bottom: 0,
              width: 'var(--mantine-spacing-xl)',
              zIndex: 300,
            }}
          >
            <ActionIcon
              variant="transparent"
              color="gray"
              style={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                opacity: sidebarOpen ? 0 : 1,
                transition: 'opacity 300ms ease',
              }}
            >
              <IconChevronRight />
            </ActionIcon>
          </Box>

          <Box
            component="aside"
            onMouseEnter={() => setSidebarOpen(true)}
            onMouseLeave={() => setSidebarOpen(false)}
            style={{
              position: 'absolute',
              top: 0,
              bottom: 0,
              width: 300,
              backgroundColor: 'white',
              padding: 'var(--mantine-spacing-md)',
              borderRadius: 'var(--mantine-radius-md)',
              boxShadow: '0 2px 8px rgba(0,0,0,0.05)',
              transform: sidebarOpen ? 'translateX(0)' : 'translateX(-100%)',
              transition: 'transform 300ms ease',
              zIndex: 200,
            }}
          >
            <Sidebar />
          </Box>

          <Box
            component="main"
            style={{
              flex: 1,
              marginLeft: sidebarOpen ? 340 : 0,
              transform: sidebarOpen ? 'scale(0.9)' : 'scale(1)',
              transformOrigin: 'left center',
              transition: 'margin-left 300ms ease, transform 300ms ease',
            }}
          >
            <MainContent />
          </Box>
        </Box>
      </AppShell.Main>
    </AppShell>
  )
}

export default App
