

CREATE TABLE IF NOT EXISTS covid_jabar(
    CLOSECONTACT BIGINT,
    CONFIRMATION BIGINT,
    PROBABLE BIGINT,
    SUSPECT BIGINT,
    closecontact_dikarantina BIGINT,
    closecontact_discarded BIGINT,
    closecontact_meninggal BIGINT,
    confirmation_sembuh BIGINT, 
    kode_kab TEXT,
    kode_prov TEXT,
    nama_kab TEXT,
    nama_prov TEXT,
    probable_diisolasi BIGINT, 
    probable_discarded BIGINT, 
    probable_meninggal BIGINT, 
    suspect_diisolasi BIGINT, 
    suspect_discarded BIGINT, 
    suspect_meninggal BIGINT,
    tanggal TEXT
);