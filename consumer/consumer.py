from nltk.sentiment.vader import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

def analyzer_function(post):
	sentiment = analyzer.polarity_scores(post)
	if(sentiment["compound"]>0.05):
		return "good"
	elif(sentiment["compound"]<0.05):
		return "bad"
	else:
		return "neutral"

