import { useState } from 'react'
import {
  Card,
  Image,
  Text,
  Group,
  Stack,
  Box,
  ScrollArea,
  Badge,
  Tooltip,
  Blockquote,
} from '@mantine/core'
import { IconQuoteFilled } from '@tabler/icons-react'
import dayjs from 'dayjs'
import type { Article } from '../types'
import { imagesApi } from '../api'

interface ArticleCardProps {
  article: Article
}

function ArticleCard({ article }: ArticleCardProps) {
  const [imageError, setImageError] = useState(false)

  const formattedDate = article.date
    ? dayjs(article.date).format('MMMM, DD YYYY')
    : ''

  const archiveName = article.archive
    ? article.archive.charAt(0).toUpperCase() + article.archive.slice(1)
    : ''

  const tag = article.tag
    ? article.tag.trim().charAt(0).toUpperCase() +
      article.tag.trim().slice(1).toLowerCase()
    : 'None'

  const imageUrl =
    article.image && !imageError
      ? imagesApi.getImageUrl(article.image)
      : 'https://placehold.co/600x400?text=Placeholder'

  return (
    <Card
      component="a"
      href={article.link}
      target="_blank"
      rel="noopener noreferrer"
      shadow="sm"
      padding="lg"
      radius="md"
      withBorder
      h="100%"
      style={{
        textDecoration: 'none',
        color: 'inherit',
        display: 'flex',
        flexDirection: 'column',
        // transition: 'transform 150ms ease, box-shadow 150ms ease',
      }}
      styles={{
        root: {
          '&:hover': {
            // transform: 'scale(1.02)',
            boxShadow: 'var(--mantine-shadow-lg)',
          },
        },
      }}
    >
      
      <Card.Section>
        <Image
          src={imageUrl}
          alt={article.title || 'Article image'}
          h={180}
          fit="cover"
          fallbackSrc="https://placehold.co/600x400?text=Placeholder"
          onError={() => setImageError(true)}
        />
      </Card.Section>

      <Stack gap="xs" mt="md" style={{ flex: 1 }}>
        <Group justify="space-between">
          <Text size="xs" c="dimmed">{archiveName}</Text>
          <Text size="xs" c="dimmed">{formattedDate}</Text>
        </Group>

        <Tooltip label={article.title || 'Untitled'} withArrow position="top" mb="1px">
          <Text fw={600} size="md" lineClamp={1} c="inky-navy.6" title={article.title || ''}>
            {article.title || 'Untitled'}
          </Text>
        </Tooltip>

        <Blockquote color="inky-navy.5" radius="md" iconSize={35} icon={
            <Box style={{ transform: "rotate(180deg)" }}>
              <IconQuoteFilled
                size={20}
                color="var(--mantine-color-inky-red-5)"
              />
            </Box>
          }
          p="sm"
        >
        <ScrollArea h={90} offsetScrollbars scrollbarSize={6}>
          <Text size="sm" c="inky-dark" lh={1.6} ta="justify">
            {article.content || "No content available"}
          </Text>
        </ScrollArea>
      </Blockquote>

        <Group>
          <Badge color="inky-red.5" variant="subtle">
            {tag}
          </Badge>
        </Group>
      </Stack>
    </Card>
  )
}

export default ArticleCard
