import { useCallback, useEffect, useState } from 'react'
import useEmblaCarousel from 'embla-carousel-react'
import { Box, Group, ActionIcon, SimpleGrid } from '@mantine/core'
import { IconChevronLeft, IconChevronRight } from '@tabler/icons-react'
import type { Article } from '../types'
import { useStore } from '../store'
import ArticleCard from './ArticleCard'

interface CarouselProps {
  articles: Article[]
  slidesPerPage: number
  maxPages: number
}

function Carousel({ articles, slidesPerPage, maxPages }: CarouselProps) {
  const {
    lastSeen,
    setPagination,
    currentSlide,
    setCurrentSlide,
  } = useStore()

  // Group articles into pages - must be before hooks that use it
  const pages: Article[][] = []
  for (let i = 0; i < articles.length; i += slidesPerPage) {
    pages.push(articles.slice(i, i + slidesPerPage))
  }
  const totalPages = pages.length

  const [emblaRef, emblaApi] = useEmblaCarousel({
    loop: false,
    align: 'start',
    slidesToScroll: 1,
    containScroll: 'trimSnaps',
  })

  const [canScrollPrev, setCanScrollPrev] = useState(false)
  const [canScrollNext, setCanScrollNext] = useState(false)
  const [selectedIndex, setSelectedIndex] = useState(0)
  const [isHovered, setIsHovered] = useState(false)

  const onSelect = useCallback(() => {
    if (!emblaApi) return
    setSelectedIndex(emblaApi.selectedScrollSnap())
    setCanScrollPrev(emblaApi.canScrollPrev())
    setCanScrollNext(emblaApi.canScrollNext())
  }, [emblaApi])

  useEffect(() => {
    if (!emblaApi) return

    onSelect()
    emblaApi.on('select', onSelect)
    emblaApi.on('reInit', onSelect)

    return () => {
      emblaApi.off('select', onSelect)
      emblaApi.off('reInit', onSelect)
    }
  }, [emblaApi, onSelect])

  // Sync carousel position when new batch loads (only when articles change)
  const articlesKey = articles.map(a => a.rowid).join(',')
  useEffect(() => {
    if (emblaApi && articles.length > 0) {
      const targetSlide = Math.min(currentSlide, totalPages - 1)
      emblaApi.scrollTo(targetSlide, false)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [emblaApi, articlesKey])

  useEffect(() => {
    if (selectedIndex !== currentSlide) {
      setCurrentSlide(selectedIndex)
    }
  }, [selectedIndex, currentSlide, setCurrentSlide])

  const scrollPrev = useCallback(() => {
    if (!emblaApi) return
    if (selectedIndex === 0 && lastSeen?.backward) {
      // At first slide, fetch previous batch
      setPagination({
        direction: 'backward',
        lastSeenDate: lastSeen.backward.date,
        lastSeenRowid: lastSeen.backward.rowid,
      })
    } else {
      emblaApi.scrollPrev()
    }
  }, [emblaApi, selectedIndex, lastSeen, setPagination])

  const scrollNext = useCallback(() => {
    if (!emblaApi) return
    if (selectedIndex === totalPages - 1 && lastSeen?.forward) {
      // At last slide, fetch next batch
      setPagination({
        direction: 'forward',
        lastSeenDate: lastSeen.forward.date,
        lastSeenRowid: lastSeen.forward.rowid,
      })
    } else {
      emblaApi.scrollNext()
    }
  }, [emblaApi, selectedIndex, totalPages, lastSeen, setPagination])

  return (
    <Box
      pos="relative"
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <Box ref={emblaRef} style={{ overflow: 'hidden' }}>
        <Box style={{ display: 'flex' }}>
          {pages.map((pageArticles, pageIndex) => (
            <Box
              key={pageIndex}
              style={{
                flex: '0 0 100%',
                minWidth: 0,
                paddingRight: 'var(--mantine-spacing-md)',
              }}
            >
              <SimpleGrid cols={slidesPerPage} spacing="lg">
                {pageArticles.map((article) => (
                  <ArticleCard key={article.rowid} article={article} />
                ))}
              </SimpleGrid>
            </Box>
          ))}
        </Box>
      </Box>

      <ActionIcon
        variant="filled"
        color="inky-red.4"
        radius="xl"
        size="lg"
        onClick={scrollPrev}
        disabled={!canScrollPrev && !lastSeen?.backward}
        pos="absolute"
        left={10}
        top="50%"
        style={{
          transform: 'translateY(-50%)',
          opacity: isHovered && (canScrollPrev || lastSeen?.backward) ? 1 : 0,
          transition: 'opacity 200ms ease',
          pointerEvents: isHovered ? 'auto' : 'none',
        }}
      >
        <IconChevronLeft size={20} />
      </ActionIcon>

      <ActionIcon
        variant="filled"
        color="inky-red.4"
        radius="xl"
        size="lg"
        onClick={scrollNext}
        disabled={!canScrollNext && !lastSeen?.forward}
        pos="absolute"
        right={10}
        top="50%"
        style={{
          transform: 'translateY(-50%)',
          opacity: isHovered && (canScrollNext || lastSeen?.forward) ? 1 : 0,
          transition: 'opacity 200ms ease',
          pointerEvents: isHovered ? 'auto' : 'none',
        }}
      >
        <IconChevronRight size={20} />
      </ActionIcon>

      <Group
        justify="center"
        gap={8}
        style={{
          position: 'absolute',
          bottom: -20,
          left: '50%',
          transform: 'translateX(-50%)',
        }}
      >
        {Array.from({ length: totalPages }).map((_, index) => (
          <Box
            key={index}
            onClick={() => emblaApi?.scrollTo(index)}
            style={{
              width: 8,
              height: 8,
              borderRadius: '50%',
              backgroundColor:
                index === selectedIndex
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
