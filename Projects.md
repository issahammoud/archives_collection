# AI Project Ideas

This repository provides a robust pipeline for collecting, visualizing, and filtering large volumes of news data. By open-sourcing this code, my goal is to help you build your portfolio by working on projects using real-world data.

Below, you’ll find two project categories: **Research** and **Development**.

---

## Development Projects

### 1. Automatic Report Generation

Automatically generating reports is a natural extension to this project. For example, you might enter the query “Israel crimes in Gaza” and your code would:

1. **Search**
   - Perform an exact (non-approximate) similarity search combined with keyword matching to maximize recall.
2. **Cluster**
   - Group the returned articles by subject and date.
3. **Summarize**
   - Feed the clustered articles into a large language model to produce concise summaries.
4. **Report**
   - Assemble a chronological timeline of event. Complete with images and source attributions in:
     - A custom HTML template, or
     - An additional tab in the existing Dash interface.

You can easily generate reports for other queries like “Israel crimes in the West Bank, in Lebanon” and so on.

> **Note:** Our database stores only excerpts from each article. To include full-text details, you may need to fetch them on demand.


### 2. Retrieval Evaluation Pipeline

Assess the quality of one or more embedding models—whether text-only or text-image measuring how well they retrieve the right content.

1. **Sample Selection**
   - Randomly pick a subset of article URLs from the database.
   - Fetch the full text for those items.

2. **Query Creation**
   - Use an LLM to generate realistic search queries based on each sampled article.
   - Optionally, translate queries into other languages to test cross-lingual retrieval.

3. **Semantic Search**
   - Run your queries against the embedding index to retrieve the top-K candidates.
   - Include both text and image modalities if evaluating a multimodal model.

4. **Evaluation Metrics**
   - Compute standard metrics such as Precision@K, Recall@K, Mean Reciprocal Rank (MRR), and so on.
   - Test “negative” queries (articles not in your database) to estimate false-positive rates.

5. **Threshold Analysis & Visualization**
   - Plot the distribution of similarity scores for true positives vs. negatives.
   - Identify score thresholds that optimize your chosen metric(s).
   - Integrate these plots into your Dash interface.

> **Challenge:** Similar articles on related topics can cause high “false” positives.


## Research Projects

### 1. Multimodal Retrieval Model Fine-Tuning

Fine-tune a joint text–image retrieval model using the news corpus (title, body, and image per article).

1. **Data Preprocessing**
   - **Length Filtering**: Remove/truncate articles whose titles or bodies are excessively short or long.
   - **Overlap Masking**: Randomly mask tokens that appear in both title and content to prevent trivial title-content matching.

2. **Model Architecture**
   - **Text Encoder**: A BERT-style transformer.
   - **Visual Encoder**: A CNN or Vision Transformer.

3. **Training Objective**
   - **Contrastive Loss**: Optimize jointly for text–text (title ↔ content) and text–image (text ↔ image) similarity.
   - **Negative Sampling**:
     - **In-batch Negatives**: Use large batch sizes to harvest hard negatives automatically.
     - **Hard-Negative Mining**: Optionally mine negatives via preprocessing (e.g., articles on similar topics).

> **Note:** Nearly all of our data collectors currently ingest articles in French, so the dataset is monolingual by default rather than multilingual.


### 2. Fine-Tune an Image Generation Model

The goal is to generate images based on article content. It's ideal for news outlets that need automatically generated, attention-grabbing images.

The core idea is to fine-tune a diffusion model conditioned on the article's title and body. While I don’t have a specific plan for this project yet, one important step is data filtering:

- **Exclude Person Images:** Since the focus is on artistic interpretation rather than factual likenesses, discard any images containing people or recognizable personalities.
- **Preprocessing:** Use a YOLO-based detector to identify and remove images with people. If this proves too aggressive, you can explore lighter-weight filters or alternative criteria.
