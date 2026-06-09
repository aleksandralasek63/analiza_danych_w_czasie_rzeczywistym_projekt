# 🚕 Monitoring i analiza floty taksówek w czasie rzeczywistym

---
### Autorzy:
Aleksandra Lasek 141179  
Maciej Rak 140918  


## 📌 Opis projektu

---

Projekt przedstawia system monitoringu i analizy danych w czasie rzeczywistym dla floty taksówek działających na terenie Warszawy.  

System symuluje flotę 100 taksówek poruszających się po mieście, wykonujących kursy oraz generujących przychody. Dane są przesyłane strumieniowo za pomocą Apache Kafka, przetwarzane przez konsumenta, a następnie prezentowane na interaktywnym dashboardzie.

Projekt obejmuje
* Symulację ruchu 100 taksówek na terenie Warszawy  
* Strumieniowanie danych do Apache Kafka  
* Monitorowanie statusu pojazdów (wolna / zajęta)  
* Śledzenie liczby wykonanych kursów  
* Analizę przychodów kierowców  
* Obliczanie przebytych kilometrów  
* Wizualizację pozycji pojazdów na mapie  
* Heat mapę przedstawiającą obszary o największym natężeniu zakończonych kursów  
* Ranking najbardziej efektywnych kierowców  
## 🧠 Cele analityczne  

---

### 💰 Monitoring przychodów kierowców  

System śledzi łączny przychód generowany przez każdą taksówkę oraz całkowity przychód całej floty.

Dzięki temu możliwe jest:

* identyfikowanie najbardziej dochodowych kierowców,  
* porównywanie efektywności pracy pojazdów,  
* monitorowanie wyników operacyjnych floty w czasie rzeczywistym.  

Potencjalne zastosowanie: analiza wydajności kierowców i planowanie systemów premiowych.

### 🚕 Monitorowanie dostępności floty

Każda taksówka posiada aktualny status:

* available – pojazd dostępny,  
* occupied – pojazd realizujący kurs.  

Dashboard prezentuje bieżącą liczbę wolnych i zajętych pojazdów.  

Potencjalne zastosowanie: ocena obciążenia floty oraz planowanie liczby pojazdów w poszczególnych strefach miasta.

### 🛣️ Analiza przebytych kilometrów

Dla każdej taksówki wyliczany jest łączny przebyty dystans na podstawie kolejnych współrzędnych geograficznych.

Dzięki temu możliwe jest:  

* monitorowanie eksploatacji pojazdów,  
* analiza aktywności kierowców,  
* porównywanie efektywności wykorzystania floty.  

Potencjalne zastosowanie: planowanie serwisów oraz analiza kosztów eksploatacji.

### 🔥 Analiza popytu – Heat Map

System rejestruje lokalizacje zakończenia kursów i zapisuje ostatnie 500 punktów popytu.  

Na dashboardzie prezentowana jest heat mapa pokazująca obszary, w których najczęściej kończą się przejazdy.  

Pozwala to identyfikować najbardziej popularne rejony miasta,
strefy o zwiększonym zapotrzebowaniu na usługi przewozowe,
potencjalne miejsca oczekiwania kierowców na kolejne zlecenia.  

Potencjalne zastosowanie: optymalizacja rozmieszczenia pojazdów oraz prognozowanie popytu.

### 🏆 Ranking najlepszych kierowców

Dashboard prezentuje ranking TOP 10 taksówek według osiągniętych przychodów.

Dla każdej pozycji wyświetlane są:

* identyfikator taksówki,  
* łączny przychód,  
* liczba wykonanych kursów,  
* przebyty dystans.  

Potencjalne zastosowanie: analiza efektywności pracy kierowców oraz monitorowanie wyników biznesowych.

## ⚙️ Architektura projektu

---

### `producer.py`

Producent danych odpowiedzialny za:

* symulację floty 100 taksówek,  
* generowanie współrzędnych geograficznych,  
* symulację kursów,  
* obliczanie przychodów,  
* obliczanie przebytych kilometrów,  
* publikowanie zdarzeń do tematu Kafka taxi-events.  

### `consumer.py`

Konsument danych odpowiedzialny za:  

* odbieranie zdarzeń z Kafka,  
* aktualizację stanu każdej taksówki,  
* agregację danych operacyjnych,  
* wyliczanie przychodów całej floty,  
* budowanie rankingu kierowców,  
* wykrywanie zakończeń kursów,  
* generowanie punktów dla heat mapy,  
* zapis danych do pliku metrics.json.  

### `dashboard.py`

Interaktywny dashboard odpowiedzialny za:  

* odczyt danych z pliku metrics.json,  
* prezentację kluczowych wskaźników KPI,  
* wizualizację pojazdów na mapie,  
* prezentację szczegółowych informacji o każdej taksówce,  
* generowanie heat mapy popytu,  
* wyświetlanie rankingu TOP 10 kierowców.  

Dashboard odświeża dane automatycznie co 10 sekund.  

## 📊 Wizualizacje

---

Projekt wykorzystuje bibliotekę Folium do prezentacji danych przestrzennych.  

Dostępne elementy wizualizacji:  

🟢 zielone znaczniki – wolne taksówki,  
🔴 czerwone znaczniki – zajęte taksówki,  
💰 przychód kierowcy,  
🛣️ przebyty dystans,  
🚕 liczba wykonanych kursów,  
🔥 heat mapa przedstawiająca obszary największego popytu.  

## Zrzut ekranu interfejsu

---
<img width="467" height="282" alt="63bfdc99-883d-482f-a4a0-70332c0d01b2" src="https://github.com/user-attachments/assets/568d28c0-23dc-403a-a5b8-569e7fa59ae5" />
<img width="378" height="245" alt="8b15736b-a4b3-4951-bff4-45e104e3bc72" src="https://github.com/user-attachments/assets/1a5b58d9-81b4-4b6a-9ecb-76b89329c606" />





## 🧠 Możliwości rozwoju 

---

📈 Predykcja popytu z wykorzystaniem modeli Machine Learning.  

🕒 Analiza ruchu w zależności od pory dnia i dnia tygodnia.  

🗺️ Wyznaczanie optymalnych stref oczekiwania dla kierowców.  

🚦 Integracja z rzeczywistymi danymi o ruchu drogowym.  

📱 Powiadomienia dla kierowców o obszarach zwiększonego zapotrzebowania.  

📊 Rozbudowany dashboard analityczny z trendami przychodów, średnią długością kursów oraz statystykami wykorzystania floty.  
