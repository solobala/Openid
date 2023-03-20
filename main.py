import requests
import time
from datetime import datetime
import pandas as pd
import logging
import warnings
from defs import defs
from pathlib import Path
warnings.simplefilter(action='ignore', category=FutureWarning)

# ## 3. 	«Первичная инициализация: переход по ссылке к списку Документов...»

# колонки датафреймов df и df_full:  Описание
#
#     no: Порядковый номер
#     title: Заголовок, берется из названия файла
#     abstract: Аннотация (вводится пропуск)
#     pub_date: Дата публикации в формате python, берется из метаданных файла («дата изменения файла» file_date)
#     web_link: Ссылка на веб-страницу, где можно получить (или прочитать) содержимое Документа
#     local_link: Локальная ссылка на загруженный файл с содержимым документа
#     text_content: Содержимое текстового файла
#     add_data: Колонка дополнительных данных, получаемых из ФПДИ. Временно заполняется пропуском
#     load_date: Дата загрузки материала в датафрейм
#

#     DOC_NAME Название источника ='openid'
#
#     КАТАЛОГИ:
#     DIR_NAME Корневой каталог 'C:\Users\Asus'
#     DIR_PATH Каталог  архивных файлов источника 'C:\Users\Asus\Downloads\openid'
#     DIR_DOCUMENTS  Каталог файлов pdf 'C:\Users\Asus\Documents\openid'
#     DIR_TXT_UP Каталог текстовых файлов и общего датафрейма 'C:\Users\Asus\Documents\Clustering_code_Doc2Vec
#     DIR_TXT Каталог текстовых файлов  'C:\Users\Asus\Documents\Clustering_code_Doc2Vec\openid'
#     DIR_FULL_DF Каталог  общего датафрейма 'C:\Users\Asus\Documents\Clustering_code_Doc2Vec\openid\df'
#     DIR_TMP Каталог сеансовых датафрреймов 'C:\Users\Asus\Documents\Clustering_code_Doc2Vec\openid\TMP'
#     log_dirname = r'C:/Users/Asus/Downloads/openid_logs'
#
#
#     ФАЙЛЫ:
#     doc_full_df - Файл общего датафрейма 'C:\Users\Asus\Documents\Clustering_code_Doc2Vec\openid\df\openid.csv'
#     doc_df - Файл сеансового датафррейма
#     'C:\Users\Asus\Documents\Clustering_code_Doc2Vec\openid\TMP\openid_yyyy-mm-dd_hh-mm-ss.csv'
#     log_filename лог  'C:\Users\Asus\Downloads\openid\openid_monitoring_{datetime.now():%Y-%m-%d_%H-%M-%S}.log'


DOC_NAME = 'openid'
DIR_NAME = Path.home()
DIR_PATH = Path(DIR_NAME, 'Downloads', DOC_NAME)

DIR_DOCUMENTS = Path(DIR_NAME, 'Documents', DOC_NAME)
DIR_TXT_UP = Path(DIR_NAME, 'Documents', 'Clustering_code_Doc2Vec')
DIR_TXT = Path(DIR_TXT_UP, DOC_NAME)
DIR_FULL_DF = Path(DIR_TXT, 'df')
doc_full_df = Path(DIR_FULL_DF, DOC_NAME + '.csv')

DIR_TMP = Path(DIR_NAME, 'Documents', 'Clustering_code_Doc2Vec', DOC_NAME, 'TMP')
f1 = DOC_NAME + f'{datetime.now():%Y-%m-%d_%H-%M-%S}.csv'
doc_df = Path(DIR_TMP, f1)
log_dirname = Path(DIR_NAME, 'Downloads', 'openid_logs')
f1 = 'openid_monitoring_' + f'{datetime.now():%Y-%m-%d_%H-%M-%S}.log'
log_filename = Path(log_dirname, f1)
cats = [DIR_PATH, DIR_DOCUMENTS, DIR_TXT_UP, DIR_TXT, DIR_FULL_DF, DIR_TMP, log_dirname]
files = [doc_full_df, log_filename]


def parser():
    # ### 3.1. Инициализация переменных статистики
    parse_time_start = datetime.now()
    doc_count = 0
    new_doc_count = 0
    new_doc_added = 0
    zip_count = 0  # ко-во обработанных ссылок
    warning_count = 0
    exception_count = 0

    # ### 3.2. Настройка логирования
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(name)s: [%(levelname)s] [%(asctime)s] %(message)s')
    current_date = datetime(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)

    try:
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.INFO)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    except FileNotFoundError:
        print("лог - файл не найден")

    logger.info(f"Текущая дата: {datetime.strftime(current_date, '%Y-%m-%d')}")
    logger.info(f'Тексты сохраняются в: {DIR_TXT}')

    # ### 3.3. Проверка существования всех необходимых для работы каталогов и файлов, создание при необходимости
    defs.is_object_exists(cats, files, logger)

    # ### 3.4.  Загрузка таблицы в датафрейм/ создание датафрейма с нуля
    # определяем  значение поля "no" в сеансовом df по общему файлу df_full  результатов обработки источника

    try:
        df_full = pd.read_csv(doc_full_df, sep='~')  # имеет такую же структуру, как и df
        no = df_full.shape[0] + 1

    except pd.errors.EmptyDataError:

        logger.debug("файл Общего датафрейма не найден. Создается пустой общий датафрейм")
        df_full = pd.DataFrame(
            columns=['no', 'title', 'file_size', 'pub_date', 'abstract', 'web_link',
                     'local_link', 'load_date', 'add_data'])
        no = 0
    except ValueError:

        logger.debug("файл Общего датафрейма не найден. Создается пустой общий датафрейм")
        df_full = pd.DataFrame(
            columns=['no', 'title', 'file_size', 'pub_date', 'abstract', 'web_link',
                     'local_link', 'load_date', 'add_data'])
        no = 0

    # Создаем пустой сеансовый датафрейм для записи результатов текущего сеанса обработки источника
    logger.debug('Создается  датафрейм для записи результатов текущего сеанса обработки источника')
    df = pd.DataFrame(
        columns=['no', 'title', 'file_size', 'pub_date', 'abstract', 'web_link', 'local_link',
                 'load_date', 'add_data'])

    # Парсинг
    # --------------------------------------------------------------------------------------------------------------------------
    # 4.	«Получение списка Документов для всех страниц Источника»
    # --------------------------------------------------------------------------------------------------------------------------
    url = 'https://openid.net/developers/specs'
    refs = defs.get_refs(url, logger)

    # 5.	«Цикл по Документам на текущей странице».
    # 5.1. Делаем в журнале запись о начале обработки страницы

    for ref in refs:
        logger.info(f'Обработка ссылки {ref}')

        # Проверяем соответствие даты последней загрузки ссылки в общем датафрейме
        last_modified = requests.get(url=ref).headers['Last-Modified']
        content_length = len(requests.get(url=ref).content)
        flag, warning_count = defs.check_dataframe(df_full, 'https://openid.net/specs', ref, last_modified, content_length, logger,
                                                   warning_count)
        zip_count += 1

        if flag:  # нужно закачивать
            # 7.3. скачиваем файл
            load_flag, df_full, df, no, new_doc_count, new_doc_added = defs.get_file(DIR_DOCUMENTS, DIR_TXT, ref,
                                                                                     'https://openid.net/specs', logger,
                                                                                     df_full, df, no, new_doc_count,
                                                                                     new_doc_added, last_modified,
                                                                                     content_length)
            # Если загрузка произошла успешно
            if load_flag:
                logger.info(f' zip_count:{zip_count}, Ссылка {ref}  обработана')
                doc_count += 1
            else:
                logger.error('не удалось скачать файл')
        else:
            logger.info('Такой файл уже есть в общем датафрейме')

        # --------------------------------------------------------------------------------------------------------------------------
        # 14.	«Есть еще страницы с необработанным списком Документов?»
        # 15.	«Переход к следующей странице с необработанным списком документов»
        # 16.	«Прерывание цикла»
        # --------------------------------------------------------------------------------------------------------------------------
        time.sleep(1)
    # --------------------------------------------------------------------------------------------------------------------------
    # 17. Передача обновленного датафрейма, данных о статистике прошедшего процесса парсинга
    # --------------------------------------------------------------------------------------------------------------------------
    # 17.1. сохраняем сеансовый датафрейм в отдельном временном файле
    if not df.empty:
        # df = df.loc[:, ['no', 'title',  'file_size', 'pub_date', 'abstract', 'web_link', 'local_link',
        #                 'load_date', 'add_data']]
        df.to_csv(f'{doc_df}', index=False, sep='~')
        # print('Сеансовый датафрейм сохранен в файле', doc_df)

    # 17.2 сохраняем df_full в отдельном временном файле

    if not df_full.empty:
        df_full.to_csv(f'{doc_full_df}', index=False, sep='~')

    parse_time_end = datetime.now()
    parse_time = parse_time_end - parse_time_start

    try:
        mean_doc_time = parse_time / new_doc_count

    except ZeroDivisionError:
        logger.info ('нет новых документов')
        mean_doc_time = 0

    # --------------------------------------------------------------------------------------------------------------------------
    # 18. Работа Основной программы
    # --------------------------------------------------------------------------------------------------------------------------
    defs.returned_data(df, parse_time, mean_doc_time, zip_count, doc_count, new_doc_count, new_doc_added, warning_count,
                       exception_count, logger)


if __name__ == '__main__':
    parser()
