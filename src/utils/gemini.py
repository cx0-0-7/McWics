from src.utils.gemini import connect_to_gemini # Import the Gemini connection function

def is_tech_internship(role, company, description):
    model = connect_to_gemini() # Using the connection function we built
    
    # Precise prompt for binary filtering
    prompt = f"""
    Analyze this internship posting for a Software Engineering student. 
    Criteria for 'YES': 
    - Requires technical skills in Java, Python, C, SQL, Linux, C++, C#, JavaScript, Bash, Rust, or other programming languages.
    - Involves coding, data analysis, system design, or cybersecurity.
    
    Criteria for 'NO':
    - Purely marketing, HR, sales, or business operations.
    - No programming mentioned.

    Role: {role}
    Company: {company}
    Description: {description}

    Return ONLY 'YES' or 'NO'. No other text.
    """
    
    try:
        response = model.generate_content(prompt)
        return response.text.strip().upper() == "YES"
    except:
        return False # Safety default