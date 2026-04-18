import os
import json
import feedparser
from supabase import create_client, Client
from groq import Groq

print("initiating orbisscope automated rss engine...")

# 1. connect to services
supabase: Client = create_client(
    os.environ.get("SUPABASE_URL"), 
    os.environ.get("SUPABASE_KEY")
)
groq_client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

# 2. the bulletproof rss feeds
RSS_FEEDS = [
    "https://www.aljazeera.com/xml/rss/all.xml",
    "http://feeds.bbci.co.uk/news/world/rss.xml"
]

def analyze_news(title, summary):
    try:
        # we force groq to output pure json so it goes straight into your database perfectly
        prompt = f"""
        analyze this news event: "{title} - {summary}"
        return ONLY a valid JSON object with these exact keys:
        "sentiment_score": a float from 0.0 to 1.0 representing geopolitical tension (1.0 is extreme war/crisis, 0.0 is total peace).
        "location_name": the specific country or city mentioned.
        "lat": approximate latitude as a float.
        "lng": approximate longitude as a float.
        "event_description": a one sentence summary.
        """

        chat_completion = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama3-8b-8192",
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        
        result = json.loads(chat_completion.choices[0].message.content)
        return result
    except Exception as e:
        print(f"groq error: {e}")
        return None

# 3. run the pipeline
processed_data = []

for url in RSS_FEEDS:
    print(f"fetching {url}")
    feed = feedparser.parse(url)
    
    # just grab the top 5 newest articles per feed to save api limits
    for entry in feed.entries[:5]:
        intel = analyze_news(entry.title, entry.description)
        
        if intel and intel.get("lat") and intel.get("lng"):
            # attach the frontend requirements
            intel["event_type"] = "rss_intercept"
            processed_data.append(intel)
            print(f"processed: {intel['location_name']} with score {intel['sentiment_score']}")

# 4. push to supabase
if processed_data:
    print(f"pushing {len(processed_data)} events to supabase...")
    supabase.table("processed_events").insert(processed_data).execute()
    print("global feed updated successfully.")