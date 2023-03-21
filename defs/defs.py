from bs4 import BeautifulSoup
import requests
import time
import re
import os
from pathlib import Path


def print_stat_dict(stat_dict, logger):
    """
    Функция печати статистики работы парсера

   Входные параметры:
   : param: stat_dict - dict ( словарь статистики
    """
    msgs = {"parse_time": '1.	Общее время (в секундах) выполнения парсинга (parse_time)',
            "mean_doc_time": '2.	Среднее время (в секундах) обработки одного документа (mean_doc_time)',
            "zip_count": '3.    Количество обработанных ссылок (zip_count)',
            "doc_count": '4.	Количество рассмотренных Документов (doc_count) ',
            "new_doc_count": '5.	Количество новых Документов (new_doc_count)',
            "new_doc_added": '6.	Количество новых Документов, добавленных в Датафрейм (new_doc_added)',
            "warning_count": '7.	Количество предупреждений (сообщение уровня WARNING - warning_count)',
            "exception_count": '8.	Количество ошибок (сообщение уровня EXCEPTION – exception_count)'
            }
    for key, value in stat_dict.items():
        # print(f'{msgs[key]}: {value}', end='\n')
        logger.info(f'{msgs[key]}: {value}')


def is_object_exists(cats, files, logger):
    """
    Функция проверяет наличие на локальном компьютере
    каталогов (cats) и файлов (files), необхдимых для работы парсера.
    Результаты отражаются в looger.

    Входные параметры:
    : param: list of strings Списки строк cats, files
    """
    # проверка существования каталогов
    for cat in cats:

        if not os.path.isdir(cat):
            logger.debug(f'{cat} не найден')

            try:
                os.makedirs(cat, mode=0o777, exist_ok=False)
                logger.debug(f'  каталог {cat} создан')

            except FileExistsError:
                logger.debug(f'{cat} был создан ранее')

    # проверка существования файлов
    for file in files:

        if not os.path.isfile(file):
            logger.debug(f'{file} не найден')

            try:
                fle = Path(file)
                fle.touch(exist_ok=True)
                logger.debug(f' Файл {file} создан')

            except FileExistsError:
                logger.debug(f'{file} был создан ранее')


def get_file(download_dir, txt_dir, ref, url, logger, df_full, df, no, new_doc_count, new_doc_added, last_modified,
             content_length):
    """
    Функция для:
    закачки страницы по ссылке ref,
    сохранения содержимого в текстовый файл,
    записи в общий и сеансовый датафреймы.
    загрузка страницы производится в dowmload_dir.
    сохранение текстового файла  в txt_dir
    Результаты отражаются в logger

    Входные параметры:
    :param download_dir
    :param txt_dir
    :param ref
    :param url
    :param logger
    :param df_full
    :param df
    :param no
    :param new_doc_count
    :param new_doc_added
    :param last_modified
    :param content_length
    :param

    return:
    load_flag, df_full, df, no,  new_doc_count, new_doc_added
    """
    load_flag = False
    try:
        ufr = requests.get(ref)  # делаем запрос

        if ufr.status_code == 200:

            # Сохраняем html - файл
            f_name = ref.replace(url + '/', '')
            txt_file_name = f_name.split('.')[0] + '.txt'
            no += 1
            local_link = Path(download_dir, f_name)
            load_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            f_html = open(str(Path(download_dir, f_name)), "wb")  # открываем файл для записи в download_dir,  wb
            f_html.write(ufr.content)  # записываем содержимое в файл
            f_html.close()

            soup = BeautifulSoup(ufr.content.decode('utf-8'), 'lxml')
            text_content = soup.get_text()
            e = ''
            numbers = []
            splitter = ''

            # Для заполнения abstract нужен текст из Abstract или Introduction
            if 'Abstract' in text_content:

                if "<h3>Abstract</h3>" in str(soup):
                    e = 'h3'

                elif "<h2>Abstract</h2>" in str(soup):
                    e = 'h2'

                elif "<h1>Abstract</h1>" in str(soup):
                    e = 'h1'

                elif soup.find(id == re.compile('abstract')).find('a').text == "Abstract":
                    # Определим имя тэга вокруг ссылки
                    e = soup.find(id == re.compile('abstract')).find('a').parent.name

                else:
                    e = soup.find(id == re.compile('abstract')).find('a').parent.name

                # Выбираем на странице все фрагменты с тэгами e и формируем из них разделитель
                h_abstract = soup.find('body').find_all(e)
                splitter = ''

                for h in h_abstract:
                    splitter = splitter + str(h) + '|'
                splitter = splitter[:-1]

                # Разделим текст на части, Определим части с активными спецификациями и сохраним их в списке numbers
                numbers = []
                for i in range(len(h_abstract)):
                    if len(re.findall('Abstract', str(h_abstract[i]))) != 0 and len(
                            re.findall('Abstract Flow', str(h_abstract[i]))) == 0:
                        numbers.append(i)

            elif 'Introduction' in text_content:
                # нужно заготовить сплиттер для Introduction

                if "<h3>Introduction</h3>" in str(soup):
                    e = 'h3'
                    h_abstract = soup.find('body').find_all(e)
                    splitter = ''
                    for h in h_abstract:
                        splitter = splitter + str(h) + '|'
                    splitter = splitter[:-1]

                    # Определим номера частей с активными спецификациями и сохраним их в списке numbers
                    numbers = []
                    for i in range(len(h_abstract)):
                        if len(re.findall('<h3>Introduction</h3>', str(h_abstract[i]))) != 0:
                            numbers.append(i)

                elif 'id="section-1-1"' in str(soup):
                    e = 'section-1-1'

            # Разделим текст body с помощью сплиттера, если это не Introduction с ссылками
            text = str(soup.body)

            if e != 'section-1-1':

                results = []
                for element in re.split(splitter, text):
                    if element is None:
                        pass
                    else:
                        results.append(element)

                abstracts = []

                for number in numbers:
                    paragrafs = BeautifulSoup(results[number + 1], 'lxml').find_all(re.compile('p|li'))
                    for p in paragrafs:

                        if str(p.text) != '':
                            if not ('<span>' in str(p)):
                                abstracts.append(
                                    p.text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ').replace("¶", " "))
                            elif p.find_all('span'):
                                abstracts.append(
                                    p.text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ').replace("¶", " "))

                abstract = ' '.join(abstracts)

            else:
                abstract = soup.body.find('p', id='section-1-1').text

            toc = ''
            if len(soup.find_all(id='toc')) > 0:
                divs = soup.find_all(id='toc')

            elif len(soup.find_all(class_='toc')) > 0:
                divs = soup.find_all(class_='toc')

            elif len(soup.find_all(id=re.compile('toc'))) > 0:
                divs = soup.find_all(id=re.compile('toc'))

            else:
                divs = []  # toc не найден

            for div in divs:
                toc = toc.join(str(div.text))

            abstract = abstract.replace('\n', ' ').replace('\t', ' ').replace("¶", " ").replace("▲", " ").replace(
                '\xa0', ' ')
            while '  ' in abstract:
                abstract = abstract.replace('  ', ' ')

            toc = toc.replace('\n', ' ').replace('\t', ' ').replace("¶", " ").replace("▲", " ").replace('\xa0', ' ')
            while '  ' in toc:
                toc = toc.replace('  ', ' ')

            text_content = text_content.replace('\n', ' ').replace('\t', ' ').replace("¶", " ").replace("▲",
                                                                                                        " ").replace(
                '\xa0', ' ')

            #  Из текста нужно удалить abstract (introduction) и содержание
            while '  ' in text_content:
                text_content = text_content.replace('  ', ' ')

            # Убираем из текста abstract и toc

            text_content = text_content.replace(abstract, '').replace('Abstract', '')
            text_content = text_content.replace(toc, '').replace('Table of Contents', '')

            # записываем текст в текстовый файл
            f_txt = open(str(Path(txt_dir, txt_file_name)), "w", encoding='UTF-8')  # открываем txt файл для записи
            f_txt.write(text_content)
            f_txt.close()

            if os.path.isfile(str(Path(download_dir, f_name))):
                logger.info(f'Файл по ссылке {ref} загружен в {download_dir}')
                new_doc_count += 1
            else:
                logger.exception(f'Файл  по ссылке {ref} не загружен')

            if os.path.isfile(str(Path(txt_dir, txt_file_name))):
                logger.info(f'Файл {txt_file_name} загружен в {txt_dir}')

            else:
                logger.exception(f'Файл  по ссылке {ref} не загружен')
            load_flag = True

            # вносим записи в сеансовый и общий датафремы
            print(len(abstract))
            df_full.loc[len(df_full.index)] = [no, f_name, content_length, last_modified, abstract, ref, local_link,
                                               load_date, '']
            df.loc[len(df_full.index)] = [no, f_name, content_length, last_modified, abstract, ref, local_link,
                                          load_date, '']
            new_doc_added += 1
        else:
            logger.error(f'Файл  по ссылке {ref} не загружен, status_code = {ufr.status_code}')

    except requests.exceptions.ConnectTimeout:
        logger.error(f'{ref} ConnectTimeout')

    except requests.exceptions.ReadTimeout:
        logger.error(f'{ref} ReadTimeout')

    except requests.exceptions.ConnectionError:
        logger.error(f'{ref} ConnectionError')

    except requests.exceptions.HTTPError:
        logger.error(f'{ref} HTTPError')

    return load_flag, df_full, df, no, new_doc_count, new_doc_added


def get_refs(url, logger):
    """
    Собирает ссылки на все страницы поисковой выдачи

    Входные параметры:
    : param url: string - URL страницы с пагинатором

    Выходные параметры:
    : return: list of string - список  ссылок
    """
    refs = []
    response = requests.get(url=url)
    response.encoding = 'UTF-8'

    if response.status_code == 200:
        soup = BeautifulSoup(response.content.decode('utf-8'), 'lxml')
        # Нам придется разбить текст фрагмента на части и взять часть с активными спецификациями. Разбивать будем по тэгам h2
        # Нам нужны только действующие спецификации и черновики, устаревшие не берем. Поэтому если
        # ссылки лежат под заголовком Obsolete|Active Drafts|Inactive Drafts, то они не нужны
        h2s = soup.find_all('div', class_='entry-content')[0].find_all('h2')
        text = str(soup.find_all('div', class_='entry-content')[0])

        splitter = ''
        for h2 in h2s:
            splitter = splitter + str(h2) + '|'
        splitter = splitter[:-1]
        results = re.split(splitter, text)
        # Определим номера частей с активными спецификациями и сохраним их в списке numbers
        numbers = []
        for i in range(len(h2s)):
            if len(re.findall('Obsolete|Active Drafts|Inactive Drafts', str(h2s[i]))) == 0:
                numbers.append(i)

        for number in numbers:
            links = BeautifulSoup(results[number + 1], 'lxml').find_all('li')
            # for link in links:
            #     refs.append(link.find('a')['href'])
            new_refs = []
            for link in links:
                if len(re.findall('See the', link.text)) == 0:
                #     print('Время собирать ссылки')
                #     print(link.text)
                #     print(link.find('a')['href'])
                #     new_refs = get_refs(str(link.find('a')['href']), logger)
                #     for element in new_refs:
                #         refs.append(element.find('a')['href'])
                # else:
                    refs.append(link.find('a')['href'])
    else:
        logger.error('Ошибка загрузки')
    return refs


def fill_text_content(title):
    """
    Функция по названию pdf-файла находит соответсвующий ему текстовый файл и возвращает его содержимое

    Входные параметры:
    :param title: String - название pdf-файла

    Выходные параметры:
    :return:
    text_content: String - содержимое текстового файла
    """
    text_content = ''
    file_name = title.split('/')[-1].split('.')[0] + '.txt'
    text_path = Path(Path.home(), 'Documents', 'Clustering_code_Doc2Vec', 'openid', file_name)

    with open(text_path, 'r', encoding='UTF-8') as file:
        try:
            text_content = file.read()
        except UnicodeDecodeError:
            print('Не удалось загрузить в датафрейм содержимое текстового файла')
    return text_content


def returned_data(df, parse_time, mean_doc_time, zip_count, doc_count, new_doc_count, new_doc_added, warning_count,
                  exception_count, logger):
    """
    Функция возвращает в основную программу 2 DataFrame:
    тексты обработанных документов в формате [text_content, web_link] , и
    статистику работы stat_dict  в формате:
    {"parse_time": parse_time,
    "mean_doc_time": mean_doc_time,
    "zip_count": zip_count,
    "doc_count": doc_count,
    "new_doc_count": new_doc_count,
    "new_doc_added": new_doc_added,
    "warning_count": warning_count,
    "exception_count": exception_count}

    Входные параметры:
    :param df: DataFrame
    :param parse_time: integer
    :param mean_doc_time:integer
    :param zip_count: integer
    :param doc_count:integer
    :param new_doc_count: integer
    :param new_doc_added:integer
    :param warning_count:integer
    :param exception_count:integer
    :param logger:журнал

    Выходные параметры:
    :return:df_returned,stat_dict:DataFrame
    """
    stat_dict = dict()
    df_returned = df[['title', 'web_link']]
    try:
        df_returned['text_content'] = df_returned.title.apply(fill_text_content)
    except:
        pass
    finally:
        df_returned.drop(columns=['title'], inplace=True)
    stat_dict['parse_time'] = parse_time
    stat_dict['mean_doc_time'] = mean_doc_time
    stat_dict['zip_count'] = zip_count
    stat_dict['doc_count'] = doc_count
    stat_dict['new_doc_count'] = new_doc_count
    stat_dict['new_doc_added'] = new_doc_added
    stat_dict['warning_count'] = warning_count
    stat_dict['exception_count'] = exception_count

    print_stat_dict(stat_dict, logger)

    return df_returned, stat_dict


def check_dataframe(df_full, url, ref, last_modified, content_length, logger, warning_count):
    """
    Проверяет совпадение данных pdf=файла с ранее загруженными по общему датафрейму
    Если check = True, загрузка нужна
    :return:
    """
    # --------------------------------------------------------------------------------------------------
    # 6. Текущий документ уже есть в датафрейме? Выполняем проверку
    # по имени pdf, размеру файла и дате последней модификации
    # --------------------------------------------------------------------------------------------------
    check = False
    if not df_full.empty:
        f_name = ref.replace(url + '/', '')

        if len(df_full.loc[(df_full.title == f_name) &
                           (df_full.pub_date == last_modified) &
                           (df_full.file_size == content_length)]) > 0:
            check = False
            logger.debug(f' Ссылка есть в таблице. Документ {f_name} пропущен')

        elif len(df_full.loc[(df_full.title == f_name) &
                             (df_full.file_size != content_length) &
                             (df_full.pub_date == last_modified)]) > 0:
            # Размер файла изменился, нужно загружать
            check = True
            logger.warning(f'{f_name} скачивается повторно: размер{content_length} !=сохраненному {df_full.file_size}')
            warning_count += 1

        elif len(df_full.loc[(df_full.title == f_name) & (df_full.file_size != content_length)]) == 0:
            # Документы ранее не скачивались, сразу загружаем
            check = True

    else:
        check = True
    return check, warning_count
