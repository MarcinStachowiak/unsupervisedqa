import spacy
import scispacy

from scispacy.linking import EntityLinker

nlp = spacy.load("en_core_web_sm")

text = """Severe acute respiratory syndrome coronavirus 2 (SARS-CoV-2) is a highly transmissible and pathogenic coronavirus that emerged in late 2019 and has caused a pandemic of acute respiratory disease, named 'coronavirus disease 2019' (COVID-19), which threatens human health and public safety. In this Review, we describe the basic virology of SARS-CoV-2, including genomic characteristics and receptor use, highlighting its key difference from previously known coronaviruses. We summarize current knowledge of clinical, epidemiological and pathological features of COVID-19, as well as recent progress in animal models and antiviral treatment approaches for SARS-CoV-2 infection. We also discuss the potential wildlife hosts and zoonotic origin of this emerging virus in detail.

"""

doc = nlp(text)

print(list(doc.sents))
print(doc.ents)
