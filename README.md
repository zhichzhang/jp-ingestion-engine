# JP Ingestion Engine

## Overview

This project is a modular web crawler and structured data ingestion pipeline designed to extract high-quality data from the Joseph Perrier winery website.

Unlike simple scrapers, this system is built to handle real-world challenges in web data extraction, including:

- Semi-structured and inconsistent HTML layouts  
- Multi-page entity assembly (winery data spread across sections)  
- Multi-language routing (French to English normalization)  
- Rich media extraction from heterogeneous sources  
- Deterministic crawling with strict deduplication  

To address these challenges, the system adopts a **modular and extensible architecture**, with a particular focus on the parsing layer. Instead of relying on a monolithic parser, it uses a **pluggable extractor-based design (Strategy Pattern)**, where each extractor encapsulates the logic for a specific field or semantic block. These extractors are orchestrated as a unified execution layer, enabling independent evolution, fault isolation, and flexible composition of parsing logic.

This design avoids tight coupling between HTML structure and parsing logic, making the system resilient to layout changes and easy to extend to new domains.

More broadly, the system is structured as a reusable crawling framework rather than a one-off scraper, with a strong emphasis on **extensibility, deterministic behavior, and resilience to unstable web structures**.

## Architecture

### High-Level Pipeline

The crawler follows a structured pipeline:

```

Seed URLs
↓
Frontier (URL queue + dedupe)
↓
Fetcher (HTTP client with retry)
↓
Parser (page classification + routing)
↓
Extractor Layer (Pluggable Strategy-based parsing)
↓
Structured Entities (Winery / Product / Media)
↓
Batch Write (persistence)

```



### Crawl Orchestration

The crawling process is coordinated by a central orchestrator that:

- Initializes the crawl with seed URLs  
- Manages concurrent fetching and parsing  
- Controls crawl scope via URL filtering  
- Handles language routing (FR → EN)  
- Buffers and flushes extracted data in batches  

The system uses a thread pool for parallel crawling, enabling efficient traversal while maintaining control over concurrency.



### Frontier (URL Scheduling)

The frontier manages the set of URLs to be crawled.

Key properties:

- Queue-based traversal (BFS-style)  
- Deduplication via normalized URLs  
- Separation between:
  - Seen URLs (already processed)  
  - Queued URLs (pending processing)  

This prevents redundant crawling and avoids infinite loops caused by URL variations such as query parameters.



### URL Normalization

URL normalization is a foundational component of the crawler.

Normalization includes:

- Removing query parameters  
- Removing fragments  
- Stripping trailing slashes  

This ensures that logically identical pages map to a single canonical URL, which is critical for:

- Deduplication  
- Crawl correctness  
- Stable data ingestion  

Media URLs use a slightly different normalization strategy to preserve meaningful query parameters when necessary.



### Fetcher (HTTP Layer)

The fetcher is responsible for retrieving page content.

Features:

- Persistent HTTP session  
- Automatic redirect handling  
- Retry with exponential backoff  
- Configurable timeout and headers  

The fetcher returns both the response body and the final resolved URL, which is used for canonicalization and deduplication.



### Language-Aware Crawling

The crawler detects and handles multilingual content.

Strategy:

- Detect French pages using HTML attributes or URL patterns  
- Extract the corresponding English version using language switch links  
- Enqueue the English page instead of parsing the French version  

This guarantees:

- Consistent language across all extracted data  
- Avoidance of duplicate ingestion across locales  



### Parser (Page Classification)

The parser routes pages to specific parsing logic based on URL patterns.

Supported page types:

- Product detail pages  
- Product catalog/listing pages  
- Winery pages (homepage, history, family)  

This routing layer ensures that:

- Each page is parsed with the correct logic  
- Parsing complexity is isolated per page type  
- The system remains extensible for additional page types  



## Extractor Architecture

### Design Overview

The core parsing logic is implemented using a modular extractor system based on the Strategy pattern, where each extractor encapsulates a specific parsing strategy for a field or semantic block.

Each extractor:

- Targets a specific field or semantic block  
- Defines its own DOM selector  
- Implements extraction logic independently  
- Returns structured data, media, and discovered URLs  

The parser composes multiple extractors dynamically and aggregates their outputs.



### Execution Model

For a given page:

- A predefined list of extractors is executed  
- Each extractor contributes partial results  
- Outputs are merged into a unified structured representation, with later extractors able to override or enrich previously extracted fields  

This design provides:

- Strong separation of concerns  
- Fault isolation (failure in one extractor does not break others)  
- Ease of extension (new fields require only a new extractor)  



### Product Extraction

Product detail pages are parsed into highly structured entities.

Extracted fields include:

- Basic identity (name, URL, description)  
- Technical attributes (dosage, aging, temperature, blend)  
- Grape composition (percentages per variety)  
- Awards and ratings (structured pairs)  
- Data sheet links (PDF)  

Many of these fields are derived from semi-structured DOM patterns such as key-value blocks or alternating nodes, requiring custom parsing logic rather than relying solely on static CSS selectors.



### Winery Extraction

Winery data is distributed across multiple pages and must be assembled incrementally.

Sources:

- Homepage: general description  
- History page: timeline of events  
- Family page: key individuals and descriptions  

Each page produces a partial record, which is later merged into a complete winery entity.



### Media Extraction

Media extraction is treated as a first-class concern.

The system extracts media from multiple sources:

- Image tags (`img`)  
- Video tags (`video`, `source`)  
- Picture elements  
- CSS background images  
- Custom attributes used by the site  

Additional logic includes:

- Resolving `srcset` to select the highest quality asset  
- Normalizing media URLs  
- Deduplicating media per page  



### Link Discovery

The crawler continuously discovers new URLs during parsing.

Sources of discovered URLs:

- Anchor tags within the page  
- Extractors emitting additional links  

Discovered URLs are:

- Converted to absolute URLs  
- Filtered to ensure they are internal  
- Normalized before being added to the frontier  



### Crawl Scope Control

To prevent uncontrolled crawling, the system restricts URLs based on allowed path prefixes.

Only relevant sections of the website are crawled, such as:

- Product listings  
- Product detail pages  
- Winery-related pages  

This ensures:

- Efficient crawling  
- No drift into irrelevant or infinite sections  



## Data Model

The system produces three primary entity types: Winery, Product, and Media.



### Winery

Represents a winery as a composite entity assembled from multiple pages.

Key attributes:

- Name and website  
- Description  
- Family structure (key individuals and roles)  
- Historical timeline (year-event pairs)  

The model supports incremental enrichment, where different pages contribute different parts of the final entity.



### Product

Represents a structured wine product with rich attributes.

Key features:

- Strong normalization of technical fields (e.g., numeric dosage instead of raw text)  
- Structured grape composition (percentages per variety)  
- Parsed awards and ratings  
- Support for semi-structured key-value extraction  

This design enables downstream querying and analysis rather than simple text storage.



### Media

Represents media assets associated with pages.

Attributes:

- Media type (image or video)  
- URL (normalized)  
- Source page URL  

Media records are deduplicated and retain provenance for traceability.



## Data Ingestion Strategy

The system follows a progressive structuring and merge-based ingestion approach:

1. Raw HTML is fetched from pages  
2. Extractors convert DOM content into structured fields  
3. Page-level results are aggregated into intermediate objects  
4. Partial records from different pages are merged  
5. Final structured entities are persisted  

A key aspect of the system is its ability to handle partial and evolving data:

- Records can be incomplete when first extracted  
- Later crawls can enrich existing data  
- Merging logic ensures no loss of information  



## Setup

### Install Dependencies

```

pip install -r requirements.txt

```



### Environment Configuration

Create a `.env` file with:

```

SUPABASE_URL=...
SUPABASE_KEY=...
BASE_URL=[https://www.josephperrier.com/](https://www.josephperrier.com/)

TIMEOUT=20
MAX_WORKERS=8
BATCH_SIZE=25

```



### Initialize Database

```

python scripts/init_db.py

```



### Run the Crawler

```

python -m src.cli.main crawl

```

You can also run the crawler directly:

```bash
python src/main.py
```

The crawler starts from the configured base URL and recursively discovers and processes relevant pages.



## CLI Usage

After setting up the environment and initializing the database, you can use the CLI to run the crawler and inspect the results.

### Run the crawler

```bash
python -m src.cli.main crawl
```

This will start crawling from the configured base URL and populate the database.



### List products

```bash
python -m src.cli.main list-products
```

You can control how many results are returned:

```bash
python -m src.cli.main list-products --limit 10
```



### Show a specific product

```bash
python -m src.cli.main show-product "cuvée royale brut"
```

This will display all stored fields for the selected product.



### Notes

* Make sure your `.env` file is correctly configured before running the CLI.
* All commands should be run from the project root directory.



## Example Run

Input:

```

[https://www.josephperrier.com/](https://www.josephperrier.com/)

```

Output:

- Structured winery data assembled from multiple pages  
- Product catalog with detailed attributes  
- Media assets extracted from across the site  
- Internal links discovered and traversed  



## Key Design Decisions

### 1. Extractor-Based Parsing

Instead of a monolithic parser, the system uses small, composable extractors.

Benefits:

- Easier maintenance  
- Independent evolution of parsing logic  
- Better fault isolation  



### 2. Deterministic Crawling

Strict URL normalization and deduplication ensure:

- No duplicate crawling  
- Stable ingestion behavior  
- Predictable crawl graph  



### 3. Structure-Aware Parsing

Many fields are not directly extractable via simple selectors.

The system handles:

- Key-value pairing  
- Alternating DOM patterns  
- Nested content structures  



### 4. Multi-Page Entity Assembly

Entities such as wineries are not confined to a single page.

The system:

- Extracts partial records  
- Merges them across pages  
- Produces a complete structured entity  



### 5. Robustness to Real-World Web Variability

The crawler is designed to handle:

- Inconsistent HTML structures  
- Missing fields  
- Layout variations across pages  

This is achieved through:

- Defensive extraction logic  
- Independent extractors  
- Merge-based data modeling  



## Future Improvements

- Generalize crawler to support multiple winery sites  
- Introduce distributed crawling (queue-based architecture)  
- Add persistent deduplication (e.g., Redis)  
- Improve observability (metrics and tracing)  
- Implement incremental recrawling strategies
