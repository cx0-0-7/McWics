from playwright.sync_api import sync_playwright
import random

def extract_job_details(job_url):
    with sync_playwright() as p:
        # Use a real-looking User Agent to bypass bot detection
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        try:
            # Increase timeout and wait for the page to actually load
            page.goto(job_url, wait_until="networkidle", timeout=60000)
            
            # 1. Get the Description
            # Try multiple common LinkedIn description selectors
            desc_selectors = ['.jobs-description-content__text', '.show-more-less-html__markup']
            description_text = "None"
            for selector in desc_selectors:
                if page.locator(selector).is_visible():
                    description_text = page.locator(selector).first.inner_text()
                    break

            # 2. Get the External Link
            apply_link = job_url # Fallback
            
            # Wait specifically for any button containing "Apply"
            apply_btn = page.get_by_role("button", name="Apply", exact=False).first
            
            if apply_btn.is_visible():
                # Check if it's "Easy Apply" (we skip these as they stay on LinkedIn)
                is_easy_apply = "Easy Apply" in apply_btn.inner_text()
                
                if not is_easy_apply:
                    with page.expect_popup() as popup_info:
                        apply_btn.click()
                    apply_link = popup_info.value.url
            
            browser.close()
            return {"description": description_text, "apply_link": apply_link}
            
        except Exception as e:
            print(f"Error for {job_url}: {e}")
            browser.close()
            return {"description": "Error", "apply_link": job_url}