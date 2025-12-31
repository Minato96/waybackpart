#!/usr/bin/env python3
"""
taaft_wayback_timeseries_FINAL_LOCKED.py

• Homepage cards across ALL Wayback eras (2015–2025+)
• STRICT extraction for quantitative metrics (no guessing)
• Heuristic extraction ONLY for pricing & release
• Progress bars, retries, rate limit, waits, checkpoint
• Monotonic: new logic only ADDS fallbacks
"""

import asyncio
import aiohttp
import pandas as pd
import time
import json
import random
import os
import re
from typing import List, Dict, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from tqdm import tqdm

# ================= CONFIG =================

TARGET_URLS = [
    "https://theresanaiforthat.com/apis/",
    "https://theresanaiforthat.com/agents/",
    "https://theresanaiforthat.com/task/3d-images/",
    "https://theresanaiforthat.com/task/3d/",
    "https://theresanaiforthat.com/task/abstract-art/",
    "https://theresanaiforthat.com/task/abstract-portraits/",
    "https://theresanaiforthat.com/task/academic-research/",
    "https://theresanaiforthat.com/task/academic-writing/",
    "https://theresanaiforthat.com/task/accessibility/",
    "https://theresanaiforthat.com/task/accounting/",
    "https://theresanaiforthat.com/task/ad-optimization/",
    "https://theresanaiforthat.com/task/adhd-advice/",
    "https://theresanaiforthat.com/task/ads/",
    "https://theresanaiforthat.com/task/adventure-games/",
    "https://theresanaiforthat.com/task/agents/",
    "https://theresanaiforthat.com/task/agile-project-management/",
    "https://theresanaiforthat.com/task/ai-advisory/",
    "https://theresanaiforthat.com/task/ai-content-detection/",
    "https://theresanaiforthat.com/task/ai-education/",
    "https://theresanaiforthat.com/task/ai-model-comparison/",
    "https://theresanaiforthat.com/task/ai-tools-search/",
    "https://theresanaiforthat.com/task/ai/",
    "https://theresanaiforthat.com/task/album-covers/",
    "https://theresanaiforthat.com/task/animal-images/",
    "https://theresanaiforthat.com/task/animations/",
    "https://theresanaiforthat.com/task/anime-characters/",
    "https://theresanaiforthat.com/task/anime-images/",
    "https://theresanaiforthat.com/task/anime/",
    "https://theresanaiforthat.com/task/answer-engine-optimization/",
    "https://theresanaiforthat.com/task/answers/",
    "https://theresanaiforthat.com/task/api-advice/",
    "https://theresanaiforthat.com/task/apis/",
    "https://theresanaiforthat.com/task/app-development-advice/",
    "https://theresanaiforthat.com/task/apps/",
    "https://theresanaiforthat.com/task/architectural-design/",
    "https://theresanaiforthat.com/task/art-prompts/",
    "https://theresanaiforthat.com/task/art/",
    "https://theresanaiforthat.com/task/article-summaries/",
    "https://theresanaiforthat.com/task/articles/",
    "https://theresanaiforthat.com/task/artistic-guidance/",
    "https://theresanaiforthat.com/task/artwork/",
    "https://theresanaiforthat.com/task/astrology/",
    "https://theresanaiforthat.com/task/avatars/",
    "https://theresanaiforthat.com/task/baby-names/",
    "https://theresanaiforthat.com/task/background-removal/",
    "https://theresanaiforthat.com/task/backgrounds/",
    "https://theresanaiforthat.com/task/beauty/",
    "https://theresanaiforthat.com/task/bible-advice/",
    "https://theresanaiforthat.com/task/bible-study/",
    "https://theresanaiforthat.com/task/blockchain-advice/",
    "https://theresanaiforthat.com/task/blogging/",
    "https://theresanaiforthat.com/task/book-covers/",
    "https://theresanaiforthat.com/task/book-recommendations/",
    "https://theresanaiforthat.com/task/book-summaries/",
    "https://theresanaiforthat.com/task/book-writing/",
    "https://theresanaiforthat.com/task/books/",
    "https://theresanaiforthat.com/task/brainstorming/",
    "https://theresanaiforthat.com/task/brand-strategies/",
    "https://theresanaiforthat.com/task/branding/",
    "https://theresanaiforthat.com/task/business-analysis/",
    "https://theresanaiforthat.com/task/business-consulting/",
    "https://theresanaiforthat.com/task/business-growth/",
    "https://theresanaiforthat.com/task/business-innovation/",
    "https://theresanaiforthat.com/task/business-intelligence/",
    "https://theresanaiforthat.com/task/business-names/",
    "https://theresanaiforthat.com/task/business-operations/",
    "https://theresanaiforthat.com/task/business-plans/",
    "https://theresanaiforthat.com/task/business-strategy/",
    "https://theresanaiforthat.com/task/business/",
    "https://theresanaiforthat.com/task/calendar/",
    "https://theresanaiforthat.com/task/calls/",
    "https://theresanaiforthat.com/task/calorie-tracking/",
    "https://theresanaiforthat.com/task/candidate-screening/",
    "https://theresanaiforthat.com/task/captions/",
    "https://theresanaiforthat.com/task/car-images/",
    "https://theresanaiforthat.com/task/card-games/",
    "https://theresanaiforthat.com/task/career-advice/",
    "https://theresanaiforthat.com/task/career/",
    "https://theresanaiforthat.com/task/caricatures/",
    "https://theresanaiforthat.com/task/cartoon-images/",
    "https://theresanaiforthat.com/task/cat-images/",
    "https://theresanaiforthat.com/task/cats/",
    "https://theresanaiforthat.com/task/character-creation/",
    "https://theresanaiforthat.com/task/character-interaction/",
    "https://theresanaiforthat.com/task/characters/",
    "https://theresanaiforthat.com/task/chatbots/",
    "https://theresanaiforthat.com/task/chatgpt-for-whatsapp/",
    "https://theresanaiforthat.com/task/chatgpt/",
    "https://theresanaiforthat.com/task/chatting-with-celebrities/",
    "https://theresanaiforthat.com/task/chatting-with-historical-figures/",
    "https://theresanaiforthat.com/task/chatting/",
    "https://theresanaiforthat.com/task/chemistry/",
    "https://theresanaiforthat.com/task/chess/",
    "https://theresanaiforthat.com/task/children-s-illustrations/",
    "https://theresanaiforthat.com/task/children-s-learning/",
    "https://theresanaiforthat.com/task/children-s-stories/",
    "https://theresanaiforthat.com/task/chinese/",
    "https://theresanaiforthat.com/task/christmas-wallpapers/",
    "https://theresanaiforthat.com/task/cinematic-images/",
    "https://theresanaiforthat.com/task/cinematic-portraits/",
    "https://theresanaiforthat.com/task/code-analysis/",
    "https://theresanaiforthat.com/task/code-conversion/",
    "https://theresanaiforthat.com/task/code-documentation/",
    "https://theresanaiforthat.com/task/code-explanations/",
    "https://theresanaiforthat.com/task/code-optimization/",
    "https://theresanaiforthat.com/task/code-reviews/",
    "https://theresanaiforthat.com/task/code-snippets/",
    "https://theresanaiforthat.com/task/coding-guidance/",
    "https://theresanaiforthat.com/task/coding-help/",
    "https://theresanaiforthat.com/task/coding-interview-preparation/",
    "https://theresanaiforthat.com/task/coding-lessons/",
    "https://theresanaiforthat.com/task/coding-mentorship/",
    "https://theresanaiforthat.com/task/coding/",
    "https://theresanaiforthat.com/task/college-admission-counseling/",
    "https://theresanaiforthat.com/task/color-palettes/",
    "https://theresanaiforthat.com/task/coloring-pages/",
    "https://theresanaiforthat.com/task/comic-book-artwork/",
    "https://theresanaiforthat.com/task/comics/",
    "https://theresanaiforthat.com/task/communication-skills/",
    "https://theresanaiforthat.com/task/communication/",
    "https://theresanaiforthat.com/task/company-analysis/",
    "https://theresanaiforthat.com/task/competitive-analysis/",
    "https://theresanaiforthat.com/task/compliance/",
    "https://theresanaiforthat.com/task/construction/",
    "https://theresanaiforthat.com/task/content-optimization/",
    "https://theresanaiforthat.com/task/content-repurposing/",
    "https://theresanaiforthat.com/task/content/",
    "https://theresanaiforthat.com/task/contract-management/",
    "https://theresanaiforthat.com/task/contract-reviews/",
    "https://theresanaiforthat.com/task/contracts/",
    "https://theresanaiforthat.com/task/conversation-analysis/",
    "https://theresanaiforthat.com/task/conversation-starters/",
    "https://theresanaiforthat.com/task/conversation-support/",
    "https://theresanaiforthat.com/task/conversational-avatars/",
    "https://theresanaiforthat.com/task/conversational-journaling/",
    "https://theresanaiforthat.com/task/conversational-management/",
    "https://theresanaiforthat.com/task/cooking-assistance/",
    "https://theresanaiforthat.com/task/copywriting/",
    "https://theresanaiforthat.com/task/courses/",
    "https://theresanaiforthat.com/task/cover-letters/",
    "https://theresanaiforthat.com/task/creativity/",
    "https://theresanaiforthat.com/task/crm/",
    "https://theresanaiforthat.com/task/crypto-advice/",
    "https://theresanaiforthat.com/task/crypto-education/",
    "https://theresanaiforthat.com/task/crypto/",
    "https://theresanaiforthat.com/task/customer-analysis/",
    "https://theresanaiforthat.com/task/customer-engagement/",
    "https://theresanaiforthat.com/task/customer-experience/",
    "https://theresanaiforthat.com/task/customer-feedback-analysis/",
    "https://theresanaiforthat.com/task/customer-support/",
    "https://theresanaiforthat.com/task/cyberpunk-images/",
    "https://theresanaiforthat.com/task/cybersecurity-advice/",
    "https://theresanaiforthat.com/task/cybersecurity/",
    "https://theresanaiforthat.com/task/d-d-assistance/",
    "https://theresanaiforthat.com/task/data-analysis/",
    "https://theresanaiforthat.com/task/data-extraction/",
    "https://theresanaiforthat.com/task/data-management/",
    "https://theresanaiforthat.com/task/data-visualization/",
    "https://theresanaiforthat.com/task/data/",
    "https://theresanaiforthat.com/task/database-q-a/",
    "https://theresanaiforthat.com/task/dating-advice/",
    "https://theresanaiforthat.com/task/dating-profiles/",
    "https://theresanaiforthat.com/task/dating/",
    "https://theresanaiforthat.com/task/debates/",
    "https://theresanaiforthat.com/task/debugging/",
    "https://theresanaiforthat.com/task/decision-making/",
    "https://theresanaiforthat.com/task/design/",
    "https://theresanaiforthat.com/task/devops/",
    "https://theresanaiforthat.com/task/diagrams/",
    "https://theresanaiforthat.com/task/diet-plans/",
    "https://theresanaiforthat.com/task/dieting/",
    "https://theresanaiforthat.com/task/discount-shopping/",
    "https://theresanaiforthat.com/task/divination/",
    "https://theresanaiforthat.com/task/diy/",
    "https://theresanaiforthat.com/task/document-analysis/",
    "https://theresanaiforthat.com/task/document-chat/",
    "https://theresanaiforthat.com/task/document-data-extraction/",
    "https://theresanaiforthat.com/task/document-management/",
    "https://theresanaiforthat.com/task/document-processing/",
    "https://theresanaiforthat.com/task/document-translation/",
    "https://theresanaiforthat.com/task/documents/",
    "https://theresanaiforthat.com/task/domain-name-ideas/",
    "https://theresanaiforthat.com/task/drawings/",
    "https://theresanaiforthat.com/task/dream-interpretation/",
    "https://theresanaiforthat.com/task/e-commerce/",
    "https://theresanaiforthat.com/task/education/",
    "https://theresanaiforthat.com/task/email-management/",
    "https://theresanaiforthat.com/task/email-marketing/",
    "https://theresanaiforthat.com/task/email-outreach/",
    "https://theresanaiforthat.com/task/email-writing/",
    "https://theresanaiforthat.com/task/email/",
    "https://theresanaiforthat.com/task/emergency-preparation/",
    "https://theresanaiforthat.com/task/emojis/",
    "https://theresanaiforthat.com/task/emotional-analysis/",
    "https://theresanaiforthat.com/task/emotional-support/",
    "https://theresanaiforthat.com/task/empathetic-conversations/",
    "https://theresanaiforthat.com/task/english-communication-improvement/",
    "https://theresanaiforthat.com/task/english-lessons/",
    "https://theresanaiforthat.com/task/english/",
    "https://theresanaiforthat.com/task/entrepreneurial-guidance/",
    "https://theresanaiforthat.com/task/essay-grading/",
    "https://theresanaiforthat.com/task/essays/",
    "https://theresanaiforthat.com/task/event-planning/",
    "https://theresanaiforthat.com/task/events/",
    "https://theresanaiforthat.com/task/exam-preparation/",
    "https://theresanaiforthat.com/task/excel-formulas/",
    "https://theresanaiforthat.com/task/excel/",
    "https://theresanaiforthat.com/task/expert-advice/",
    "https://theresanaiforthat.com/task/exterior-design/",
    "https://theresanaiforthat.com/task/faceless-videos/",
    "https://theresanaiforthat.com/task/fact-checking/",
    "https://theresanaiforthat.com/task/fantasy-football-advice/",
    "https://theresanaiforthat.com/task/fantasy-images/",
    "https://theresanaiforthat.com/task/fantasy-worlds/",
    "https://theresanaiforthat.com/task/fashion-advice/",
    "https://theresanaiforthat.com/task/fashion-images/",
    "https://theresanaiforthat.com/task/fashion/",
    "https://theresanaiforthat.com/task/file-conversion/",
    "https://theresanaiforthat.com/task/file-management/",
    "https://theresanaiforthat.com/task/filmmaking/",
    "https://theresanaiforthat.com/task/finance/",
    "https://theresanaiforthat.com/task/financial-advice/",
    "https://theresanaiforthat.com/task/financial-analysis/",
    "https://theresanaiforthat.com/task/financial-management/",
    "https://theresanaiforthat.com/task/financial-markets-q-a/",
    "https://theresanaiforthat.com/task/financial-planning/",
    "https://theresanaiforthat.com/task/fitness-coaching/",
    "https://theresanaiforthat.com/task/fitness/",
    "https://theresanaiforthat.com/task/flashcards/",
    "https://theresanaiforthat.com/task/floor-plans/",
    "https://theresanaiforthat.com/task/food-analysis/",
    "https://theresanaiforthat.com/task/food-images/",
    "https://theresanaiforthat.com/task/french-lessons/",
    "https://theresanaiforthat.com/task/frontend-development/",
    "https://theresanaiforthat.com/task/fundraising/",
    "https://theresanaiforthat.com/task/funny-conversations/",
    "https://theresanaiforthat.com/task/funny-images/",
    "https://theresanaiforthat.com/task/future-baby-images/",
    "https://theresanaiforthat.com/task/game-assets/",
    "https://theresanaiforthat.com/task/game-characters/",
    "https://theresanaiforthat.com/task/game-strategies/",
    "https://theresanaiforthat.com/task/games/",
    "https://theresanaiforthat.com/task/gaming-coach/",
    "https://theresanaiforthat.com/task/gardening/",
    "https://theresanaiforthat.com/task/german-lessons/",
    "https://theresanaiforthat.com/task/gifs/",
    "https://theresanaiforthat.com/task/gift-ideas/",
    "https://theresanaiforthat.com/task/global-news-summaries/",
    "https://theresanaiforthat.com/task/goals/",
    "https://theresanaiforthat.com/task/gothic-art/",
    "https://theresanaiforthat.com/task/gpt-customization/",
    "https://theresanaiforthat.com/task/gpt-recommendations/",
    "https://theresanaiforthat.com/task/gpts/",
    "https://theresanaiforthat.com/task/grading/",
    "https://theresanaiforthat.com/task/graffiti-images/",
    "https://theresanaiforthat.com/task/grammar/",
    "https://theresanaiforthat.com/task/graphic-design/",
    "https://theresanaiforthat.com/task/guides/",
    "https://theresanaiforthat.com/task/hairstyles/",
    "https://theresanaiforthat.com/task/health/",
    "https://theresanaiforthat.com/task/healthcare-documentation/",
    "https://theresanaiforthat.com/task/healthcare/",
    "https://theresanaiforthat.com/task/historical-images/",
    "https://theresanaiforthat.com/task/history-lessons/",
    "https://theresanaiforthat.com/task/history/",
    "https://theresanaiforthat.com/task/homework/",
    "https://theresanaiforthat.com/task/horror-images/",
    "https://theresanaiforthat.com/task/hr/",
    "https://theresanaiforthat.com/task/html/",
    "https://theresanaiforthat.com/task/humor/",
    "https://theresanaiforthat.com/task/icons/",
    "https://theresanaiforthat.com/task/ielts/",
    "https://theresanaiforthat.com/task/illustrations/",
    "https://theresanaiforthat.com/task/image-analysis/",
    "https://theresanaiforthat.com/task/image-captions/",
    "https://theresanaiforthat.com/task/image-descriptions/",
    "https://theresanaiforthat.com/task/image-editing/",
    "https://theresanaiforthat.com/task/image-organization/",
    "https://theresanaiforthat.com/task/image-prompts/",
    "https://theresanaiforthat.com/task/image-recreation/",
    "https://theresanaiforthat.com/task/image-search/",
    "https://theresanaiforthat.com/task/image-to-text/",
    "https://theresanaiforthat.com/task/image-to-video/",
    "https://theresanaiforthat.com/task/image-upscaling/",
    "https://theresanaiforthat.com/task/images/",
    "https://theresanaiforthat.com/task/immigration-advice/",
    "https://theresanaiforthat.com/task/influencer-marketing/",
    "https://theresanaiforthat.com/task/infographics/",
    "https://theresanaiforthat.com/task/information-retrieval/",
    "https://theresanaiforthat.com/task/instagram/",
    "https://theresanaiforthat.com/task/interactive-games/",
    "https://theresanaiforthat.com/task/interactive-learning/",
    "https://theresanaiforthat.com/task/interior-design/",
    "https://theresanaiforthat.com/task/interview-preparation/",
    "https://theresanaiforthat.com/task/investment-advice/",
    "https://theresanaiforthat.com/task/investment-research/",
    "https://theresanaiforthat.com/task/invoices/",
    "https://theresanaiforthat.com/task/japanese-lessons/",
    "https://theresanaiforthat.com/task/javascript/",
    "https://theresanaiforthat.com/task/job-adverts/",
    "https://theresanaiforthat.com/task/job-applications/",
    "https://theresanaiforthat.com/task/job-descriptions/",
    "https://theresanaiforthat.com/task/job-interviews/",
    "https://theresanaiforthat.com/task/job-search/",
    "https://theresanaiforthat.com/task/jokes/",
    "https://theresanaiforthat.com/task/journaling/",
    "https://theresanaiforthat.com/task/knowledge-bases/",
    "https://theresanaiforthat.com/task/knowledge-maps/",
    "https://theresanaiforthat.com/task/knowledge/",
    "https://theresanaiforthat.com/task/landing-pages/",
    "https://theresanaiforthat.com/task/landscapes/",
    "https://theresanaiforthat.com/task/language-learning/",
    "https://theresanaiforthat.com/task/large-language-models/",
    "https://theresanaiforthat.com/task/lead-generation/",
    "https://theresanaiforthat.com/task/leadership-development/",
    "https://theresanaiforthat.com/task/learning/",
    "https://theresanaiforthat.com/task/leftovers-recipes/",
    "https://theresanaiforthat.com/task/legal-advice/",
    "https://theresanaiforthat.com/task/legal-drafting/",
    "https://theresanaiforthat.com/task/legal/",
    "https://theresanaiforthat.com/task/lesson-plans/",
    "https://theresanaiforthat.com/task/life-advice/",
    "https://theresanaiforthat.com/task/life-coaching/",
    "https://theresanaiforthat.com/task/line-art/",
    "https://theresanaiforthat.com/task/linkedin/",
    "https://theresanaiforthat.com/task/linux/",
    "https://theresanaiforthat.com/task/literature-analysis/",
    "https://theresanaiforthat.com/task/literature/",
    "https://theresanaiforthat.com/task/live-translations/",
    "https://theresanaiforthat.com/task/logos/",
    "https://theresanaiforthat.com/task/macos/",
    "https://theresanaiforthat.com/task/management/",
    "https://theresanaiforthat.com/task/manga-creation/",
    "https://theresanaiforthat.com/task/manga/",
    "https://theresanaiforthat.com/task/market-research/",
    "https://theresanaiforthat.com/task/marketing-campaigns/",
    "https://theresanaiforthat.com/task/marketing-content/",
    "https://theresanaiforthat.com/task/marketing-strategies/",
    "https://theresanaiforthat.com/task/marketing/",
    "https://theresanaiforthat.com/task/math-lessons/",
    "https://theresanaiforthat.com/task/math-problems/",
    "https://theresanaiforthat.com/task/math/",
    "https://theresanaiforthat.com/task/meal-plans/",
    "https://theresanaiforthat.com/task/medical-advice/",
    "https://theresanaiforthat.com/task/medical-documentation/",
    "https://theresanaiforthat.com/task/meditation/",
    "https://theresanaiforthat.com/task/meeting-summaries/",
    "https://theresanaiforthat.com/task/meetings/",
    "https://theresanaiforthat.com/task/memes/",
    "https://theresanaiforthat.com/task/mental-health/",
    "https://theresanaiforthat.com/task/mind-maps/",
    "https://theresanaiforthat.com/task/miniature-world-images/",
    "https://theresanaiforthat.com/task/minimalist-art/",
    "https://theresanaiforthat.com/task/models/",
    "https://theresanaiforthat.com/task/motivation/",
    "https://theresanaiforthat.com/task/motivational-coach/",
    "https://theresanaiforthat.com/task/motivational-quotes/",
    "https://theresanaiforthat.com/task/movie-posters/",
    "https://theresanaiforthat.com/task/movie-recommendations/",
    "https://theresanaiforthat.com/task/movies-tv-shows-recommendations/",
    "https://theresanaiforthat.com/task/movies/",
    "https://theresanaiforthat.com/task/music-covers/",
    "https://theresanaiforthat.com/task/music-discovery/",
    "https://theresanaiforthat.com/task/music-lyrics/",
    "https://theresanaiforthat.com/task/music-production/",
    "https://theresanaiforthat.com/task/music-recommendations/",
    "https://theresanaiforthat.com/task/music/",
    "https://theresanaiforthat.com/task/names/",
    "https://theresanaiforthat.com/task/nature-images/",
    "https://theresanaiforthat.com/task/networking/",
    "https://theresanaiforthat.com/task/news/",
    "https://theresanaiforthat.com/task/newsletters/",
    "https://theresanaiforthat.com/task/nightlife-recommendations/",
    "https://theresanaiforthat.com/task/notes/",
    "https://theresanaiforthat.com/task/nutrition/",
    "https://theresanaiforthat.com/task/operating-systems/",
    "https://theresanaiforthat.com/task/outfits/",
    "https://theresanaiforthat.com/task/paintings/",
    "https://theresanaiforthat.com/task/paraphrasing/",
    "https://theresanaiforthat.com/task/parenting/",
    "https://theresanaiforthat.com/task/patents/",
    "https://theresanaiforthat.com/task/pattern-images/",
    "https://theresanaiforthat.com/task/pdfs/",
    "https://theresanaiforthat.com/task/personal-assistant/",
    "https://theresanaiforthat.com/task/personal-development/",
    "https://theresanaiforthat.com/task/personal-finance/",
    "https://theresanaiforthat.com/task/personal/",
    "https://theresanaiforthat.com/task/personalized-workouts/",
    "https://theresanaiforthat.com/task/pet-care/",
    "https://theresanaiforthat.com/task/pets/",
    "https://theresanaiforthat.com/task/philosophical-conversations/",
    "https://theresanaiforthat.com/task/philosophical-guidance/",
    "https://theresanaiforthat.com/task/photography-mentoring/",
    "https://theresanaiforthat.com/task/photography/",
    "https://theresanaiforthat.com/task/physics/",
    "https://theresanaiforthat.com/task/pickup-lines/",
    "https://theresanaiforthat.com/task/pitch-decks/",
    "https://theresanaiforthat.com/task/pitches/",
    "https://theresanaiforthat.com/task/pixel-art/",
    "https://theresanaiforthat.com/task/plant-identification/",
    "https://theresanaiforthat.com/task/playlists/",
    "https://theresanaiforthat.com/task/podcasts/",
    "https://theresanaiforthat.com/task/poetry/",
    "https://theresanaiforthat.com/task/poker/",
    "https://theresanaiforthat.com/task/portraits/",
    "https://theresanaiforthat.com/task/powerpoint-presentations/",
    "https://theresanaiforthat.com/task/presentations/",
    "https://theresanaiforthat.com/task/press-releases/",
    "https://theresanaiforthat.com/task/problem-solving/",
    "https://theresanaiforthat.com/task/procurement/",
    "https://theresanaiforthat.com/task/product-comparison/",
    "https://theresanaiforthat.com/task/product-descriptions/",
    "https://theresanaiforthat.com/task/product-design/",
    "https://theresanaiforthat.com/task/product-development/",
    "https://theresanaiforthat.com/task/product-images/",
    "https://theresanaiforthat.com/task/product-listing-optimization/",
    "https://theresanaiforthat.com/task/product-listings/",
    "https://theresanaiforthat.com/task/product-management/",
    "https://theresanaiforthat.com/task/product-reviews/",
    "https://theresanaiforthat.com/task/product-videos/",
    "https://theresanaiforthat.com/task/productivity/",
    "https://theresanaiforthat.com/task/professional-avatars/",
    "https://theresanaiforthat.com/task/project-management/",
    "https://theresanaiforthat.com/task/project-planning/",
    "https://theresanaiforthat.com/task/prompt-optimization/",
    "https://theresanaiforthat.com/task/prompts/",
    "https://theresanaiforthat.com/task/prospecting/",
    "https://theresanaiforthat.com/task/public-relations/",
    "https://theresanaiforthat.com/task/python-lessons/",
    "https://theresanaiforthat.com/task/python/",
    "https://theresanaiforthat.com/task/qr-codes/",
    "https://theresanaiforthat.com/task/questions-generation/",
    "https://theresanaiforthat.com/task/quizzes/",
    "https://theresanaiforthat.com/task/rap-music/",
    "https://theresanaiforthat.com/task/react/",
    "https://theresanaiforthat.com/task/reading/",
    "https://theresanaiforthat.com/task/real-estate-assistance/",
    "https://theresanaiforthat.com/task/real-estate-evaluation/",
    "https://theresanaiforthat.com/task/real-estate/",
    "https://theresanaiforthat.com/task/recipes/",
    "https://theresanaiforthat.com/task/recruiting/",
    "https://theresanaiforthat.com/task/regex/",
    "https://theresanaiforthat.com/task/relationship-advice/",
    "https://theresanaiforthat.com/task/relationships/",
    "https://theresanaiforthat.com/task/reputation-management/",
    "https://theresanaiforthat.com/task/research-papers-summaries/",
    "https://theresanaiforthat.com/task/research/",
    "https://theresanaiforthat.com/task/restaurant-recommendations/",
    "https://theresanaiforthat.com/task/resume-analysis/",
    "https://theresanaiforthat.com/task/resume-optimization/",
    "https://theresanaiforthat.com/task/resumes/",
    "https://theresanaiforthat.com/task/retro-images/",
    "https://theresanaiforthat.com/task/review-management/",
    "https://theresanaiforthat.com/task/reviews/",
    "https://theresanaiforthat.com/task/riddles/",
    "https://theresanaiforthat.com/task/risk-assessment/",
    "https://theresanaiforthat.com/task/running/",
    "https://theresanaiforthat.com/task/sales-training/",
    "https://theresanaiforthat.com/task/sales/",
    "https://theresanaiforthat.com/task/sarcastic-conversations/",
    "https://theresanaiforthat.com/task/school-subjects/",
    "https://theresanaiforthat.com/task/school/",
    "https://theresanaiforthat.com/task/sci-fi-illustrations/",
    "https://theresanaiforthat.com/task/scriptwriting/",
    "https://theresanaiforthat.com/task/sculptures/",
    "https://theresanaiforthat.com/task/search/",
    "https://theresanaiforthat.com/task/security/",
    "https://theresanaiforthat.com/task/seo-advice/",
    "https://theresanaiforthat.com/task/seo-content/",
    "https://theresanaiforthat.com/task/seo-keywords/",
    "https://theresanaiforthat.com/task/seo-metadata/",
    "https://theresanaiforthat.com/task/seo/",
    "https://theresanaiforthat.com/task/shakespeare/",
    "https://theresanaiforthat.com/task/shopping/",
    "https://theresanaiforthat.com/task/short-stories/",
    "https://theresanaiforthat.com/task/short-videos/",
    "https://theresanaiforthat.com/task/sketch-to-image/",
    "https://theresanaiforthat.com/task/sketches/",
    "https://theresanaiforthat.com/task/sleep/",
    "https://theresanaiforthat.com/task/social-media-captions/",
    "https://theresanaiforthat.com/task/social-media-comments/",
    "https://theresanaiforthat.com/task/social-media-engagement/",
    "https://theresanaiforthat.com/task/social-media-posts/",
    "https://theresanaiforthat.com/task/social-media/",
    "https://theresanaiforthat.com/task/software-testing/",
    "https://theresanaiforthat.com/task/software/",
    "https://theresanaiforthat.com/task/source-referencing/",
    "https://theresanaiforthat.com/task/space-images/",
    "https://theresanaiforthat.com/task/spanish-lessons/",
    "https://theresanaiforthat.com/task/speeches/",
    "https://theresanaiforthat.com/task/spiritual-guidance/",
    "https://theresanaiforthat.com/task/spirituality/",
    "https://theresanaiforthat.com/task/spooky-images/",
    "https://theresanaiforthat.com/task/sports/",
    "https://theresanaiforthat.com/task/spreadsheets/",
    "https://theresanaiforthat.com/task/sql/",
    "https://theresanaiforthat.com/task/startup-advice/",
    "https://theresanaiforthat.com/task/startup-idea-validation/",
    "https://theresanaiforthat.com/task/startup-ideas/",
    "https://theresanaiforthat.com/task/startups/",
    "https://theresanaiforthat.com/task/stick-figures/",
    "https://theresanaiforthat.com/task/stickers/",
    "https://theresanaiforthat.com/task/stocks/",
    "https://theresanaiforthat.com/task/stoic-advice/",
    "https://theresanaiforthat.com/task/stories/",
    "https://theresanaiforthat.com/task/storyboards/",
    "https://theresanaiforthat.com/task/storybooks/",
    "https://theresanaiforthat.com/task/storytelling-game/",
    "https://theresanaiforthat.com/task/strategic-advice/",
    "https://theresanaiforthat.com/task/strategic-conversations/",
    "https://theresanaiforthat.com/task/studying/",
    "https://theresanaiforthat.com/task/summaries/",
    "https://theresanaiforthat.com/task/surreal-art/",
    "https://theresanaiforthat.com/task/surveys/",
    "https://theresanaiforthat.com/task/svg-illustrations/",
    "https://theresanaiforthat.com/task/swift-coding/",
    "https://theresanaiforthat.com/task/t-shirt-designs/",
    "https://theresanaiforthat.com/task/tarot-card-readings/",
    "https://theresanaiforthat.com/task/task-automation/",
    "https://theresanaiforthat.com/task/task-management/",
    "https://theresanaiforthat.com/task/tattoos/",
    "https://theresanaiforthat.com/task/tax/",
    "https://theresanaiforthat.com/task/teaching/",
    "https://theresanaiforthat.com/task/team-collaboration/",
    "https://theresanaiforthat.com/task/team-management/",
    "https://theresanaiforthat.com/task/tech-insights/",
    "https://theresanaiforthat.com/task/tech-support/",
    "https://theresanaiforthat.com/task/technical-interview-preparation/",
    "https://theresanaiforthat.com/task/test-automation/",
    "https://theresanaiforthat.com/task/text-enhancement/",
    "https://theresanaiforthat.com/task/text-extraction/",
    "https://theresanaiforthat.com/task/text-humanization/",
    "https://theresanaiforthat.com/task/text-rewriting/",
    "https://theresanaiforthat.com/task/text-to-speech/",
    "https://theresanaiforthat.com/task/text-translation/",
    "https://theresanaiforthat.com/task/text/",
    "https://theresanaiforthat.com/task/therapy/",
    "https://theresanaiforthat.com/task/tiktok/",
    "https://theresanaiforthat.com/task/time-management/",
    "https://theresanaiforthat.com/task/topic-simplification/",
    "https://theresanaiforthat.com/task/trading/",
    "https://theresanaiforthat.com/task/transcription/",
    "https://theresanaiforthat.com/task/translations/",
    "https://theresanaiforthat.com/task/travel-guides/",
    "https://theresanaiforthat.com/task/travel-itineraries/",
    "https://theresanaiforthat.com/task/travel-plans/",
    "https://theresanaiforthat.com/task/travel-recommendations/",
    "https://theresanaiforthat.com/task/travel/",
    "https://theresanaiforthat.com/task/trend-analysis/",
    "https://theresanaiforthat.com/task/trivia-games/",
    "https://theresanaiforthat.com/task/tutorials/",
    "https://theresanaiforthat.com/task/tweets/",
    "https://theresanaiforthat.com/task/twitter/",
    "https://theresanaiforthat.com/task/typography/",
    "https://theresanaiforthat.com/task/ui-design/",
    "https://theresanaiforthat.com/task/university/",
    "https://theresanaiforthat.com/task/user-personas/",
    "https://theresanaiforthat.com/task/user-stories/",
    "https://theresanaiforthat.com/task/ux-design/",
    "https://theresanaiforthat.com/task/vehicle-diagnosis/",
    "https://theresanaiforthat.com/task/vibe-coding/",
    "https://theresanaiforthat.com/task/video-avatars/",
    "https://theresanaiforthat.com/task/video-captions/",
    "https://theresanaiforthat.com/task/video-dubbing/",
    "https://theresanaiforthat.com/task/video-editing/",
    "https://theresanaiforthat.com/task/video-scripts/",
    "https://theresanaiforthat.com/task/video-summaries/",
    "https://theresanaiforthat.com/task/video-transcription/",
    "https://theresanaiforthat.com/task/video-translation/",
    "https://theresanaiforthat.com/task/videos/",
    "https://theresanaiforthat.com/task/viral-content/",
    "https://theresanaiforthat.com/task/viral-videos/",
    "https://theresanaiforthat.com/task/virtual-companions/",
    "https://theresanaiforthat.com/task/virtual-employees/",
    "https://theresanaiforthat.com/task/virtual-try-ons/",
    "https://theresanaiforthat.com/task/vocabulary-improvement/",
    "https://theresanaiforthat.com/task/voice-agents/",
    "https://theresanaiforthat.com/task/voice-changing/",
    "https://theresanaiforthat.com/task/voice-cloning/",
    "https://theresanaiforthat.com/task/wallpapers/",
    "https://theresanaiforthat.com/task/watercolor-images/",
    "https://theresanaiforthat.com/task/watermark-removal/",
    "https://theresanaiforthat.com/task/wealth/",
    "https://theresanaiforthat.com/task/web-browsing/",
    "https://theresanaiforthat.com/task/web-design/",
    "https://theresanaiforthat.com/task/web-scraping/",
    "https://theresanaiforthat.com/task/website-analysis/",
    "https://theresanaiforthat.com/task/website-optimization/",
    "https://theresanaiforthat.com/task/website-summaries/",
    "https://theresanaiforthat.com/task/websites/",
    "https://theresanaiforthat.com/task/wellness/",
    "https://theresanaiforthat.com/task/whatsapp/",
    "https://theresanaiforthat.com/task/wine-recommendations/",
    "https://theresanaiforthat.com/task/wordpress/",
    "https://theresanaiforthat.com/task/workflows/",
    "https://theresanaiforthat.com/task/writing-assistance/",
    "https://theresanaiforthat.com/task/writing-enhancement/",
    "https://theresanaiforthat.com/task/writing/",
    "https://theresanaiforthat.com/task/youtube-channel-optimization/",
    "https://theresanaiforthat.com/task/youtube-scripts/",
    "https://theresanaiforthat.com/task/youtube-summaries/",
    "https://theresanaiforthat.com/task/youtube-thumbnails/"
]

BASE_URL = "https://theresanaiforthat.com/"

OUT_CSV = "taaft_tools_timeseries_tasks_api_agents.csv"
CHECKPOINT_FILE = "wayback_checkpoint.json"

WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_FROM = "2015"
WAYBACK_TO = "2025"
WAYBACK_FILTER = "statuscode:200"

REQUESTS_PER_MIN = 60
TIMEOUT = 30
MAX_RETRIES = 10
BACKOFF_BASE = 1.7
SAVE_EVERY = 50
SNAPSHOT_DELAY = 0.3

# ================= RATE LIMITER =================

class RateLimiter:
    def __init__(self, rate):
        self.capacity = rate
        self.tokens = rate
        self.last = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.time()
                elapsed = now - self.last
                self.tokens = min(
                    self.capacity,
                    self.tokens + elapsed * (self.capacity / 60)
                )
                self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            await asyncio.sleep(0.05)

RATE_LIMITER = RateLimiter(REQUESTS_PER_MIN)

# ================= HELPERS =================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
]

PRICE_RE = re.compile(r"(free|from\s*\$\d+|\$\d+)", re.I)
RELATIVE_RE = re.compile(r"\b\d+\s*(h|d|w|m|y)\s*ago\b", re.I)

def headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

def safe_int(txt):
    try:
        return int(txt.replace(",", "").strip())
    except:
        return None

def safe_float(txt):
    try:
        return float(txt.strip())
    except:
        return None

def first_text(li, selectors: List[str]) -> Optional[str]:
    for sel in selectors:
        el = li.select_one(sel)
        if el:
            txt = el.get_text(strip=True)
            if txt:
                return txt
    return None

def first_attr(li, selectors: List[str], attr: str) -> Optional[str]:
    for sel in selectors:
        el = li.select_one(sel)
        if el and el.get(attr):
            return el.get(attr)
    return None

# ================= STRICT METRIC EXTRACTORS =================
# (NO GUESSING — ONLY REAL TAGS)

def extract_saves(li):
    el = li.select_one(".saves")
    return safe_int(el.text) if el else None

def extract_comments(li):
    for sel in [
        ".comments"
    ]:
        el = li.select_one(sel)
        if el:
            return safe_int(el.text)
    return None

def extract_rating(li):
    # 1️⃣ Canonical (2024–2025)
    for sel in [
        ".average_rating"
    ]:
        el = li.select_one(sel)
        if el:
            return safe_float(el.text)

    # 2️⃣ 2023 stats_right pattern (e.g. JungGPT)
    for sel in [
        ".stats .stats_right",
    ]:
        el = li.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            # match exactly one decimal rating like 5.0, 4.3, etc.
            m = re.search(r"\b\d\.\d\b", txt)
            if m:
                return float(m.group(0))

    return None


def extract_views(li):
    for sel in [
        ".stats_views span",
        ".views_count",
        ".views_count_count"
    ]:
        el = li.select_one(sel)
        if el:
            return safe_int(el.text)
    return None

# ================= SEMANTIC (SAFE) EXTRACTORS =================

def extract_pricing(li):
    # Newest / structured
    txt = first_text(li, [".ai_launch_date"])
    if txt:
        return txt

    # Early 2023
    txt = first_text(li, [".tags .tag.price"])
    if txt:
        return txt

    # Late 2022
    txt = first_text(li, [".tags .price"])
    if txt:
        return txt

    return None

def extract_release(li):
    # 2024–2025 relative release
    txt = first_text(li, [".released .relative"])
    if txt:
        return txt

    # Older absolute release
    txt = first_text(li, [".available_starting"])
    if txt:
        return txt.replace("Released", "").strip()

    # Attribute-based (2025)
    if li.get("data-release"):
        return li.get("data-release")

    return None

def extract_tags(li):
    tags = []
    for el in li.select(".tags span"):
        t = el.get_text(strip=True)
        if t and not PRICE_RE.search(t):
            tags.append(t)
    return ",".join(tags) if tags else None

def extract_video(li):
    if "has_video" not in li.get("class", []):
        return False

    wrapper = li.select_one(".main_video_embed_wrapper")
    if not wrapper:
        return False

    iframe = wrapper.select_one("iframe")
    poster = wrapper.select_one(".video_poster")
    views = wrapper.select_one(".views_count_count")

    thumbnail = None
    if poster and poster.get("style"):
        m = re.search(r"url\(['\"]?(.*?)['\"]?\)", poster.get("style"))
        if m:
            thumbnail = m.group(1)

    return {
        "title": iframe.get("title") if iframe else None,
        "embed_url": iframe.get("src") if iframe else None,
        "provider": (
            "mediadelivery"
            if iframe and "mediadelivery" in iframe.get("src", "")
            else None
        ),
        "thumbnail": thumbnail,
        "views": safe_int(views.text) if views else None
    }


def extract_comments_json(li):
    if "has_comment" not in li.get("class", []):
        return False

    comments = []
    for c in li.select(".comment"):
        comments.append({
            "comment_id": c.get("data-id"),
            "user": {
                "user_id": c.get("data-user"),
                "username": first_text(c, [".user_name"]),
                "profile_url": first_attr(c, [".user_card"], "href")
            },
            "date": first_text(c, [".comment_date a"]),
            "body": first_text(c, [".comment_body"]),
            "upvotes": safe_int(first_text(c, [".comment_upvote"])) or 0
        })

    return comments if comments else False



# ================= NETWORK =================

async def fetch(session, url, context=""):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await RATE_LIMITER.acquire()
            async with session.get(url, headers=headers(), timeout=TIMEOUT) as r:
                if r.status == 200:
                    return await r.text(errors="ignore"), None
                wait = BACKOFF_BASE ** attempt
                tqdm.write(f"[retry {attempt}] {context} HTTP {r.status} → wait {wait:.1f}s")
                await asyncio.sleep(wait)
        except Exception as e:
            last_err = e
            wait = BACKOFF_BASE ** attempt
            tqdm.write(f"[retry {attempt}] {context} exception → wait {wait:.1f}s")
            await asyncio.sleep(wait)
    return None, str(last_err)

async def query_cdx(session, target_url):
    q = (
        f"{WAYBACK_CDX_URL}?url={target_url}"
        f"&from={WAYBACK_FROM}&to={WAYBACK_TO}"
        f"&output=json&filter={WAYBACK_FILTER}&collapse=timestamp"
    )
    text, _ = await fetch(session, q, f"CDX {target_url}")
    data = json.loads(text)
    return [
        {
            "timestamp": row[1],
            "snapshot_url": f"https://web.archive.org/web/{row[1]}/{target_url}",
            "source_url": target_url
        }
        for row in data[1:]
    ]


# ================= PARSER =================

def parse_snapshot(html, ts, snap_url,source_url):
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for li in soup.select("li.li"):
        r = {
            "snapshot_timestamp": ts,
            "snapshot_url": snap_url,
            "tool_id": li.get("data-id"),
            "tool_name": li.get("data-name") or first_text(li, ["a.ai_link"]),
            "raw_classes": " ".join(li.get("class", [])),
            "internal_link": None,
            "external_link": None,
            "tags": extract_tags(li),
            "pricing_text": extract_pricing(li),
            "release_text": extract_release(li),
            "saves": extract_saves(li),
            "comments": extract_comments(li),
            "rating": extract_rating(li),
            "views": extract_views(li),
            "video": extract_video(li),
            "comments_json": extract_comments_json(li),
            "is_verified": "verified" in li.get("class", []),
            "is_pinned": "is_pinned" in li.get("class", []),
            "share_slug": first_attr(li, [".share_ai"], "data-slug"),
            "source_url": source_url,
        }

        internal = first_attr(li, ["a.ai_link[href*='/ai/']"], "href")
        external = first_attr(
            li,
            ["a.external_ai_link", "a.ai_link[target='_blank']"],
            "href"
        )

        if internal:
            r["internal_link"] = urljoin(BASE_URL, internal)
        if external:
            r["external_link"] = urljoin(BASE_URL, external)

        rows.append(r)

    return rows

# ================= CHECKPOINT =================

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE) as f:
        return set(json.load(f))

def save_checkpoint(done):
    with open(CHECKPOINT_FILE + ".tmp", "w") as f:
        json.dump(list(done), f)
    os.replace(CHECKPOINT_FILE + ".tmp", CHECKPOINT_FILE)

# ================= MAIN =================

async def main():
    async with aiohttp.ClientSession() as session:
        snapshots = []
        for url in TARGET_URLS:
            snaps = await query_cdx(session, url)
            snapshots.extend(snaps)

        # sort AFTER collecting all snapshots
        snapshots.sort(key=lambda x: (x["source_url"], x["timestamp"]))

        done = load_checkpoint()

        snap_bar = tqdm(total=len(snapshots), desc="Snapshots")
        row_bar = tqdm(desc="Rows")

        buffer = []

        for snap in snapshots:
            ts = snap["timestamp"]
            key = f"{snap['source_url']}|{ts}"

            if key in done:
                snap_bar.update(1)
                continue

            html, _ = await fetch(session, snap["snapshot_url"], f"snapshot {ts}")
            if html:
                rows = parse_snapshot(
                        html,
                        ts,
                        snap["snapshot_url"],
                        snap["source_url"]
                    )
                buffer.extend(rows)
                row_bar.update(len(rows))

            done.add(key)
            snap_bar.update(1)
            snap_bar.set_postfix(buf=len(buffer))

            if len(buffer) >= SAVE_EVERY:
                pd.DataFrame(buffer).to_csv(
                    OUT_CSV,
                    mode="a",
                    header=not os.path.exists(OUT_CSV),
                    index=False
                )
                buffer.clear()
                save_checkpoint(done)

            await asyncio.sleep(SNAPSHOT_DELAY)

        if buffer:
            pd.DataFrame(buffer).to_csv(
                OUT_CSV,
                mode="a",
                header=not os.path.exists(OUT_CSV),
                index=False
            )
            save_checkpoint(done)

        snap_bar.close()
        row_bar.close()

if __name__ == "__main__":
    asyncio.run(main())
