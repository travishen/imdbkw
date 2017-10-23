# imdb-spider
Create a movie database with imdb films for Plot Keywords anaysis

Usage
-----

Initialize database:

    $ python imdbkw.py --dburl postgresql+psycopg2://username:password@host/dbname --setup

After initialization, you can generate sample movies and keywords from imdb like:

    >>> import imdbkw
    >>> imdbkw.setup_engine('postgresql+psycopg2://username:password@host/dbname')
    >>> number_of_films_per_genre = 10
    >>> imdbkw.process_film(number_of_films_per_genre)
    >>> number_of_films
    >>> imdbkw.process_keyword(number_of_films)
