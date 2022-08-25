# Databricks notebook source
# MAGIC %sh
# MAGIC pip install openai

# COMMAND ----------

# Import libraries
import os
import openai
import json

# COMMAND ----------

# Set OpenAI API Key
OPENAI_API_KEY = ""
openai.api_key = OPENAI_API_KEY

# COMMAND ----------

# Request the text needed. In this time we want to simulate some reviews about sports-football category podcasts with the rating associated with it.
response = openai.Completion.create(
  model="text-davinci-002",
  prompt="Write 20 opinions about a podcast in \"sports-football\" category. Rate each opinion with a number between 0 and 5. The response should be in a json file structure.",
  temperature=0.7,
  max_tokens=549,
  top_p=1,
  frequency_penalty=0,
  presence_penalty=0
)

# COMMAND ----------

# Retrieve the body of the response

text = response['choices'][0]['text']
text

# COMMAND ----------

# Save json file for further processing

with open('/dbfs/FileStore/SAME/source2/open_ai_opinions.json', 'w') as f:
    json.dump(text, f)