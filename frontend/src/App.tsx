import { useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AppShell, Box, Grid } from '@mantine/core'
import Header from './components/Header'
import SearchBar from './components/SearchBar'
import MainContent from './components/MainContent'
import StatsPanel from './components/StatsPanel'
import ControlPanel from './components/ControlPanel'
import { useStore } from './store'
import { articlesApi } from './api'

function App() {
  const { setFilters } = useStore()

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
        {/* Search Bar - Central, 60% width */}
        <SearchBar />

        {/* Main Content Area - Split 70/30 */}
        <Box px="md">
          <Grid gutter="xl">
            {/* Left Panel - 70% - Carousel with white background */}
            <Grid.Col span={8}>
              <MainContent />
            </Grid.Col>

            {/* Right Panel - 30% - Stats and Controls with gray background */}
            <Grid.Col span={4}>
              <Box style={{ display: 'flex', gap: 'var(--mantine-spacing-md)' }}>
                <Box style={{ flex: 1 }}>
                  <StatsPanel />
                </Box>
                <ControlPanel />
              </Box>
            </Grid.Col>
          </Grid>
        </Box>
      </AppShell.Main>
    </AppShell>
  )
}

export default App
