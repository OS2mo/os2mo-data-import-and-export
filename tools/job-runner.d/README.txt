Job-runner.d er en måde, hvorpå man kan trække komponenter ud af job-runner.sh og definere 
alt tilhørende den enkelte komponent i en fil

OBS
OBS
OBS

Indtil videre findes alt i job-runner.sh selv - men på et tidspunkt bør alt 
flyttes hertil og den rækkefølge, der er i job-runner erstattes af den rækkefølge som
numrene i filnavnene beskriver.

Dette beskriver indtil videre køreplanen for denne flytning, som IKKE er pågående endnu

Alle jobnavne skal være med bindestreger og IKKE underscores

en jobfil for jobbet ``hent-nye-data-test`` vil hedde 0100-hent-nye-data-test.sh 
filen job-runner.d/0100-hent-nye-data-test.sh kan se ud som følger

---------------------------------------------------------------------

hent_nye_data_test_variabel1="eventuelle variable har underscores"
hent_nye_data_test_variabel2="og er prefixede så man kan se forskel"


hent-nye-data-test-precheck(){

    echo her checkes for betingelser for jobbet
    og der returneres 0 for ok og 1 for fejl
    alle interne funktioner er prefixede
    lige præcis denne kaldes automatisk hvis den findes
}

hent-nye-data-test(){

    BACK_UP_BEFORE_JOBS+=(en_fil_der_skal_tages_backup_af_inden_kørslen)
    BACK_UP_AND_TRUNCATE+=(en_fil_der_skal_bakkes_op_og_trunkeres_efter_kørslen)
    BACK_UP_AFTER_JOBS+=(en_fil_der_skal_bakkes_op_efter_kørslen)

    echo - dette er jobbet - og nu er det kørt
}

job-runner-sequence-add RUN_HENT_NYE_DATA_TEST "${BASH_SOURCE}"

---------------------------------------------------------------------

Ovenstående bør være nok til at beskrive et job og få det ind på den
forudbestemte plads i rækkefølgen
