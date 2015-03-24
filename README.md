# Структура репозитория:
```
├── Makefile            - Позволяет собрать libmydb
├── gen_workload        - Генератор тестов
├── runner
│   ├── Makefile	- Makefile для собрки и тестирования программы загрузки
│   ├── database.cpp
│   ├── database.h
│   ├── main.cpp
│   └── test.sh
├── src			- Исходные файлы libmydb
```

# Тестирование:

В папке runner:
make test_rwd
make test_rwd_silent

