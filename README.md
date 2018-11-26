# Apollo

- Apollo is a web scraping project for non-static (continuously updated) data over various sources.
- This project is intended to be a standalone application rather than a framework.
- One can easily track changes of product prices or movie ratings over time.

#### Getting Started

##### Install
`git pull https://github.com/dorukcan/apollo.git`

`cd apollo`

`pip install -r requirements.txt`

##### Settings
Create a copy of `/apollo/settings_default.py` to `/apollo/settings.py`.

##### Run

`bash run.sh` 
or 
`venv/bin/activate && python -m apollo`


#### Folder Hierarchy
    backend
        cache.py
        crawler.py
        database.py
        engine.py
        exceptions.py
        extractor.py
        message.py
        site.py
        utils.py
    crawlers
        market
            a101.py
            bizimtoptan.py
            carrefoursa.py
            hm.py
            kitapyurdu.py
            lcwaikiki.py
            migros.py
            sahibinden.py
            watsons.py
        social
            imdb.py
            play_store.py
            tweeter.py
            vikipedi.py
            youtube.py
        todo
            hepsiburada.py
            instagram.py
            mango.py
            modem.py
            stackexchange.py
            trendyol.py
            yemeksepeti.py
        google_trends.py
    modules
        market.py
        social.py
    sql
        cache.sql
        db.sql
        messages.sql
        market.sql
        social.sql
    __main__.py
    settings.py
    tasks.py

#### License

This project is licensed under the MIT License - see the LICENSE.md file for details.
