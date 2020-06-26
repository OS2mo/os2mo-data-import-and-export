imports_sd_update_primary(){
    BACK_UP_AND_TRUNCATE+=(
        "${DIPEXAR}/calculate_primary.log"
    )
    echo updating primary engagements
    ${VENV}/bin/python3 integrations/SD_Lon/calculate_primary.py --recalculate-all || (
        # denne fejl skal ikke stoppe afviklingen, da en afbrudt kørsel blot kan gentages
        echo FEJL i updating primary engagements, men kører videre
    )
}

