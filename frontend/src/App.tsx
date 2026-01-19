import { useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AppShell, Box, Grid, Text, Stack } from '@mantine/core'
import { useMediaQuery } from '@mantine/hooks'
import Header from './components/Header'
import SearchBar from './components/SearchBar'
import MainContent from './components/MainContent'
import StatsPanel from './components/StatsPanel'
import ControlPanel from './components/ControlPanel'
import { useStore } from './store'
import { articlesApi } from './api'

function App() {
  const { setFilters } = useStore()
  const isMobile = useMediaQuery('(max-width: 768px)')

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
    <AppShell header={{ height: isMobile ? 60 : 80 }}>
      <AppShell.Header withBorder>
        <Header />
      </AppShell.Header>

      <AppShell.Main
        bg="#f9fafb"
        // style={{
        //   minHeight: 'calc(100vh - var(--app-shell-header-height, 0px))',
        //   display: 'flex',
        //   flexDirection: 'column',
        //   paddingBottom: 0,
        // }}
      >
        <Box style={{ flex: 1 }}>
          {isMobile ? (
            // Mobile Layout
            <Stack gap="md" px="xs">
              <SearchBar />
              <StatsPanel isMobile />
              <MainContent isMobile />
            </Stack>
          ) : (
            // Desktop Layout
            <>
              <SearchBar />
              <Box px="md">
                <Grid gutter="xl" grow align="center" justify="center">
                  <Grid.Col span={8}>
                    <MainContent />
                  </Grid.Col>
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
            </>
          )}
        </Box>
      </AppShell.Main>
    </AppShell>
  )
}

export default App
