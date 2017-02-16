FROM onsdigital/dp-go

WORKDIR /app/

COPY ./build/dp-dd-csv-transformer .

ENTRYPOINT ./dp-dd-csv-transformer
