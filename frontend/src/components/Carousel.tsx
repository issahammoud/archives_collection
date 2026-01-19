import { useCallback, useState, useEffect } from 'react'
import { Box, Group, ActionIcon, SimpleGrid, ScrollArea } from '@mantine/core'
import { IconChevronLeft, IconChevronRight } from '@tabler/icons-react'
import type { Article } from '../types'
import { useStore } from '../store'
import ArticleCard from './ArticleCard'
import ArticlePreviewCard from './ArticlePreviewCard'
import { Grid } from '@mantine/core'

interface CarouselProps {
  articles: Article[]
  slidesPerPage: number
  maxPages: number
  isMobile?: boolean
}

const DESKTOP_CARDS_PER_PAGE = 9

function Carousel({ articles, slidesPerPage, maxPages, isMobile = false }: CarouselProps) {
  const {
    lastSeen,
    setPagination,
    currentSlide,
    setCurrentSlide,
  } = useStore()

  // For mobile: 1 article per page, for desktop: 9 articles per page
  const cardsPerPage = isMobile ? 1 : DESKTOP_CARDS_PER_PAGE

  // Group articles into pages
  const pages: Article[][] = []
  for (let i = 0; i < articles.length; i += cardsPerPage) {
    pages.push(articles.slice(i, i + cardsPerPage))
  }
  const totalPages = pages.length

  // Current page index
  const [pageIndex, setPageIndex] = useState(0)
  // Selected card index within the current page (0-8)
  const [selectedCardIndex, setSelectedCardIndex] = useState(0)
  // For fade animation
  const [isTransitioning, setIsTransitioning] = useState(false)

  const [isHovered, setIsHovered] = useState(false)

  // Handle card selection with fade animation
  const handleCardSelect = (index: number) => {
    if (index === selectedCardIndex) return
    setIsTransitioning(true)
    setTimeout(() => {
      setSelectedCardIndex(index)
      setIsTransitioning(false)
    }, 150)
  }

  // Get current page articles
  const currentPageArticles = pages[pageIndex] || []
  // Get the selected article for the big card
  const selectedArticle = currentPageArticles[selectedCardIndex]

  // Sync page index when currentSlide changes from store
  useEffect(() => {
    if (currentSlide !== pageIndex) {
      setPageIndex(currentSlide)
      setSelectedCardIndex(0)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentSlide])

  // Reset selection when articles change
  const articlesKey = articles.map(a => a.rowid).join(',')
  useEffect(() => {
    setSelectedCardIndex(0)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [articlesKey])

  const canScrollPrev = pageIndex > 0 || !!lastSeen?.backward
  const canScrollNext = pageIndex < totalPages - 1 || !!lastSeen?.forward

  // Calculate which page to land on when fetching previous batch
  const lastPageIndex = isMobile ? 19 : 4 // 20 pages for mobile (0-19), 5 pages for desktop (0-4)

  const scrollPrev = useCallback(() => {
    if (pageIndex === 0 && lastSeen?.backward) {
      // At first page, fetch previous batch
      // Set position immediately to last page (where we'll land)
      setPageIndex(lastPageIndex)
      setCurrentSlide(lastPageIndex)
      setSelectedCardIndex(0)
      setPagination({
        direction: 'backward',
        lastSeenDate: lastSeen.backward.date,
        lastSeenRowid: lastSeen.backward.rowid,
      })
    } else if (pageIndex > 0) {
      const newIndex = pageIndex - 1
      setPageIndex(newIndex)
      setCurrentSlide(newIndex)
      setSelectedCardIndex(0)
    }
  }, [pageIndex, lastSeen, setPagination, setCurrentSlide, lastPageIndex])

  const scrollNext = useCallback(() => {
    if (pageIndex === totalPages - 1 && lastSeen?.forward) {
      // At last page, fetch next batch
      // Set position immediately to first page (where we'll land)
      setPageIndex(0)
      setCurrentSlide(0)
      setSelectedCardIndex(0)
      setPagination({
        direction: 'forward',
        lastSeenDate: lastSeen.forward.date,
        lastSeenRowid: lastSeen.forward.rowid,
      })
    } else if (pageIndex < totalPages - 1) {
      const newIndex = pageIndex + 1
      setPageIndex(newIndex)
      setCurrentSlide(newIndex)
      setSelectedCardIndex(0)
    }
  }, [pageIndex, totalPages, lastSeen, setPagination, setCurrentSlide])

  const goToPage = useCallback((index: number) => {
    setPageIndex(index)
    setCurrentSlide(index)
    setSelectedCardIndex(0)
  }, [setCurrentSlide])

  // For mobile, the current article is just the first (and only) article on the page
  const currentArticle = isMobile ? currentPageArticles[0] : selectedArticle

  if (!currentArticle) {
    return null
  }

  // Mobile Layout
  if (isMobile) {
    return (
      <Box pos="relative" pb="xl">
        {/* Navigation buttons - positioned outside the card */}
        {canScrollPrev && (
          <ActionIcon
            variant="filled"
            color="inky-red.4"
            radius="xl"
            size="md"
            onClick={scrollPrev}
            pos="absolute"
            left={10}
            top="40%"
            style={{
              transform: 'translateX(-100%) translateY(-50%)',
              zIndex: 10,
            }}
          >
            <IconChevronLeft size={16} />
          </ActionIcon>
        )}

        {canScrollNext && (
          <ActionIcon
            variant="filled"
            color="inky-red.4"
            radius="xl"
            size="md"
            onClick={scrollNext}
            pos="absolute"
            right={10}
            top="40%"
            style={{
              transform: 'translateX(100%) translateY(-50%)',
              zIndex: 10,
            }}
          >
            <IconChevronRight size={16} />
          </ActionIcon>
        )}

        {/* Single Article Card - same width as stats panel */}
        <Box
          bg="gray"
          style={{ borderRadius: 'var(--mantine-radius-md)' }}
        >
          <ArticleCard article={currentArticle} />
        </Box>

        {/* Page indicator dots - scrollable for mobile */}
        <ScrollArea
          type="never"
          style={{
            marginTop: 16,
          }}
        >
          <Group
            justify="center"
            gap={4}
            wrap="nowrap"
            style={{
              minWidth: 'max-content',
              padding: '0 16px',
            }}
          >
            {Array.from({ length: totalPages }).map((_, index) => (
              <Box
                key={index}
                onClick={() => goToPage(index)}
                style={{
                  width: 6,
                  height: 6,
                  borderRadius: '50%',
                  backgroundColor:
                    index === pageIndex
                      ? 'var(--mantine-color-inky-red-4)'
                      : 'var(--mantine-color-gray-3)',
                  cursor: 'pointer',
                  transition: 'background-color 150ms ease',
                  flexShrink: 0,
                }}
              />
            ))}
          </Group>
        </ScrollArea>
      </Box>
    )
  }

  // Desktop Layout
  return (
    <Box
      pos="relative"
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <Grid gutter="xl" align="stretch">
        {/* Left side: 9 small preview cards in 3 columns */}
        <Grid.Col span={5}>
          <SimpleGrid cols={3} spacing="sm">
            {currentPageArticles.map((article, index) => (
              <ArticlePreviewCard
                key={article.rowid}
                article={article}
                isSelected={index === selectedCardIndex}
                onClick={() => handleCardSelect(index)}
              />
            ))}
          </SimpleGrid>
        </Grid.Col>

        <Grid.Col span={7}>
          <Box
            style={{
              height: '100%',
            }}
          >
            <ArticleCard article={currentArticle} />
          </Box>
        </Grid.Col>
      </Grid>

      {/* Navigation buttons - only render when navigation is possible */}
      {canScrollPrev && (
        <ActionIcon
          variant="filled"
          color="inky-red.4"
          radius="xl"
          size="lg"
          onClick={scrollPrev}
          pos="absolute"
          left={-20}
          top="50%"
          style={{
            transform: 'translateY(-50%)',
            opacity: isHovered ? 1 : 0,
            transition: 'opacity 200ms ease',
            pointerEvents: isHovered ? 'auto' : 'none',
            zIndex: 10,
          }}
        >
          <IconChevronLeft size={20} />
        </ActionIcon>
      )}

      {canScrollNext && (
        <ActionIcon
          variant="filled"
          color="inky-red.4"
          radius="xl"
          size="lg"
          onClick={scrollNext}
          pos="absolute"
          right={-20}
          top="50%"
          style={{
            transform: 'translateY(-50%)',
            opacity: isHovered ? 1 : 0,
            transition: 'opacity 200ms ease',
            pointerEvents: isHovered ? 'auto' : 'none',
            zIndex: 10,
          }}
        >
          <IconChevronRight size={20} />
        </ActionIcon>
      )}

      {/* Page indicator dots */}
      <Group
        justify="center"
        gap={8}
        style={{
          position: 'absolute',
          bottom: -24,
          left: '50%',
          transform: 'translateX(-50%)',
        }}
      >
        {Array.from({ length: totalPages }).map((_, index) => (
          <Box
            key={index}
            onClick={() => goToPage(index)}
            style={{
              width: 6,
              height: 6,
              borderRadius: '50%',
              backgroundColor:
                index === pageIndex
                  ? 'var(--mantine-color-inky-red-4)'
                  : 'var(--mantine-color-gray-3)',
              cursor: 'pointer',
              transition: 'background-color 150ms ease',
            }}
          />
        ))}
      </Group>
    </Box>
  )
}

export default Carousel
