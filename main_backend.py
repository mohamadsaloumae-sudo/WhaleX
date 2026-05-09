import os
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import google.generativeai as genai

# تحميل المفاتيح السرية من ملف .env الموجود في السيرفر
load_dotenv()

app = FastAPI(title="WhaleX AI Backend")

# السماح للميني آب بالاتصال بالسيرفر
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- إعدادات الذكاء الاصطناعي ---
GEMINI_KEY = os.getenv("GEMINI_API_KEY")
GROQ_KEY = os.getenv("GROQ_API_KEY")

# تهيئة Gemini
genai.configure(api_key=GEMINI_KEY)
gemini_model = genai.GenerativeModel('gemini-1.5-flash')

class ChatRequest(BaseModel):
    message: str

@app.post("/api/ai-assistant")
async def ai_assistant(request: ChatRequest):
    prompt = f"""أنت مساعد تداول خبير في نظام WhaleX. أجب باختصار (3-5 أسطر) واحترافية. 
    سؤال المستخدم: {request.message}"""
    
    # 🟢 المحاولة الأولى: نظام Gemini (الأساسي)
    try:
        response = gemini_model.generate_content(prompt)
        return {"status": "success", "reply": response.text, "provider": "Gemini"}
        
    except Exception as gemini_error:
        print(f"⚠️ Gemini is busy or failed: {gemini_error}. Switching to Groq...")
        
        # ⚡ المحاولة الثانية: التحويل التلقائي إلى Groq (الاحتياطي)
        try:
            headers = {
                "Authorization": f"Bearer {GROQ_KEY}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": "llama3-70b-8192", # أسرع وأذكى نموذج مجاني في Groq
                "messages": [{"role": "user", "content": prompt}]
            }
            
            groq_res = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload)
            groq_data = groq_res.json()
            
            reply = groq_data['choices'][0]['message']['content']
            return {"status": "success", "reply": reply, "provider": "Groq"}
            
        except Exception as groq_error:
            print(f"❌ Groq also failed: {groq_error}")
            raise HTTPException(status_code=500, detail="الذكاء الاصطناعي يواجه ضغطاً كبيراً حالياً، يرجى المحاولة بعد ثوانٍ.")

# تشغيل السيرفر
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
