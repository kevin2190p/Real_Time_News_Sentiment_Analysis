import re
import requests
from bs4 import BeautifulSoup
import logging

# Setup logging configuration at module level if desired
logging.basicConfig(level=logging.INFO)

class TextCleaner:
    # List of common non-article keywords to filter out (from Task 1)
    NON_ARTICLE_KEYWORDS = [
        "subscription", "subscribe", "comment", "comments", "create a display name", "Follow Al Jazeera English",
        "Sponsored", "edited by", "Sign up", "name", "email", "website", "news", "offer",
        "Email address", "Follow", "info", "Your bid", "proceed", "inbox", "receive", "Thank you for your report!",
        "Your daily digest", "Search", "Review", "Reviews", "Car Launches", "Driven Communications Sdn. Bhd.", "200801035597 (836938-P)",
        "Follow", "Email address", "Sign up", "For more of the latest", "subscribing", "2025 Hearst Magazines, Inc. .",
        "Connect", "enjoy", "love", "Best", "The Associated Press", "NBCUniversal Media, LLC", 
        "Reporting by", "Contact", "ResearchAndMarkets.com", "Advertisement", "thank you"
    ]

    # List of common FAQ-related terms to filter out (from Task 1)
    FAQ_KEYWORDS = [
        "faq", "frequently asked questions", "how to", "questions", "help", "contact us", "support", "terms and conditions",
        "privacy policy", "cookie policy", "all rights reserved", "disclaimer", "sitemap", "legal", "copyright"
    ]

    logger = logging.getLogger(__name__)
    
    """
    Provides methods to clean input text, split it into sentences,
    and fetch and clean article content.
    """
    
    @staticmethod
    def clean_text(text):
        """
        Cleans the input text.
        """
        if text is None or not isinstance(text, str):
            return ""
        
        text = re.sub(r'[^\x00-\x7F]+', '', text)
        text = re.sub(
            r'(©|All Rights Reserved|Privacy Policy|Terms and Conditions|Cookie Policy|Disclaimer)',
            '', text
        )
        text = re.sub(r'[^a-zA-Z0-9\s.,?!;:()\'"-]', '', text)
        
        for keyword in TextCleaner.NON_ARTICLE_KEYWORDS:
            text = re.sub(
                r'[^.?!]*\b' + re.escape(keyword) + r'\b[^.?!]*[.?!]',
                '', text, flags=re.IGNORECASE
            )
        
        for faq in TextCleaner.FAQ_KEYWORDS:
            text = re.sub(
                r'\b' + re.escape(faq) + r'\b',
                '', text, flags=re.IGNORECASE
            )
        
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    @staticmethod
    def split_into_sentences(text):
        """
        Splits text into sentences.
        """
        if text is None or not isinstance(text, str) or not text.strip():
            return []
        
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
        sentences = [s.strip() for s in sentences if s.strip() and len(s.strip()) > 10]
        return sentences

    @staticmethod
    def fetch_and_clean_article_content(url):
        """
        Fetches article content from a URL and cleans it.
        """
        try:
            response = requests.get(url, timeout=30)
            
            # Skip if status code is 429, 401, or 405
            if response.status_code in [429, 401, 405]:
                return ""
            
            # Check if the content type is HTML and response is OK
            if 'text/html' in response.headers.get('Content-Type', '') and response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Remove non-article sections
                for footer in soup.find_all(
                    ['footer', 'aside', 'nav', 'form', 'section', 'div', 'span'], 
                    class_=['subscription', 'newsletter', 'comments', 'related-articles',
                             'advertisement', 'popup', 'banner', 'sponsored', 'more-articles',
                             'alerts', 'social-media']
                ):
                    footer.decompose()
                
                # Remove image captions, metadata, or unnecessary content
                for img in soup.find_all(['img', 'figure', 'figcaption']):
                    img.decompose()
                
                # Remove specific non-article content
                for non_article in soup.find_all(
                    ['div', 'span', 'section'], 
                    class_=['topic', 'acknowledgment', 'external-source', 'time-zone', 
                           'multilingual', 'search', 'alerts']
                ):
                    non_article.decompose()
                
                # Remove promotional content
                for promo in soup.find_all(
                    ['div', 'span', 'section'],
                    class_=['manage-alerts', 'article-commenting', 'breaking-news', 
                           'article-comments', 'affiliate-links']
                ):
                    promo.decompose()
                
                # Remove all header tags
                for header in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                    header.decompose()
                
                # Remove all <a> tags (hyperlinks)
                for a_tag in soup.find_all('a'):
                    a_tag.decompose()
                
                # Remove all <li> tags (list items)
                for li_tag in soup.find_all('li'):
                    li_tag.decompose()
                
                # Find the first <p> tag and the last <p> tag
                first_paragraph = soup.find('p')
                all_paragraphs = soup.find_all('p')
                last_paragraph = all_paragraphs[-1] if all_paragraphs else None
                
                if first_paragraph and last_paragraph:
                    page_text = "\n".join(
                        [para.get_text(separator=' ', strip=True) 
                         for para in first_paragraph.find_all_next('p')
                         if para != last_paragraph and para.get_text(strip=True)]
                    )
                    page_text += "\n" + last_paragraph.get_text(separator=' ', strip=True)
                else:
                    page_text = ""
                
                # Use the clean_text static method via the class
                return TextCleaner.clean_text(page_text)
        except Exception as e:
            TextCleaner.logger.error(f"Error fetching article content from {url}: {str(e)}")
        
        return ""
