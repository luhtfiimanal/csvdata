package main

import (
	"fmt"
	"time"

	"github.com/luhtfiimanal/csvdata"
)

func main() {
	starttime := time.Now().UTC().UnixMilli()
	cfg := csvdata.CsvAggregateTableConfigs{
		FileConfigs: []csvdata.FileConfig{
			{
				FileNamingFormat: "./tes/data/tanah_2006-01-02.csv",
				FileFrequency:    "24h",
			},
			{
				FileNamingFormat: "./tes/data/angkasa_2006-01-02.csv",
				FileFrequency:    "24h",
			},
		},
		Requests: []csvdata.RequestColumnTable{
			// sun duration
			{InputColumnName: "SunDurMin_Tot", OutputColumnName: "SunDur_08-16", Method: csvdata.MEAN, WindowString: "8h_16h"},
			{InputColumnName: "SunDurMin_Tot", OutputColumnName: "SunDur_08-18", Method: csvdata.MEAN, WindowString: "8h_18h"},
			{InputColumnName: "SlrW_Avg", OutputColumnName: "SolarRad_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "SlrW_Avg", OutputColumnName: "SolarRad_Real", Method: csvdata.LAST},

			// air pressure
			{InputColumnName: "AP_1200_Avg", OutputColumnName: "AP_0", Method: csvdata.PICK, PickRelative: "0h"},
			{InputColumnName: "AP_1200_Avg", OutputColumnName: "AP_REAL", Method: csvdata.LAST},

			// rain
			{InputColumnName: "Rain_Tot", OutputColumnName: "Rain_07", Method: csvdata.SUM, WindowString: "-16h59m59s_7h"},
			{InputColumnName: "Rain_Tot", OutputColumnName: "Rain_0730", Method: csvdata.SUM, WindowString: "7h_7h30m"},
			{InputColumnName: "Rain_Tot", OutputColumnName: "Rain_1330", Method: csvdata.SUM, WindowString: "7h30m_13h30m"},
			{InputColumnName: "Rain_Tot", OutputColumnName: "Rain_1730", Method: csvdata.SUM, WindowString: "13h30m_17h30m"},
			{InputColumnName: "Rain_Tot", OutputColumnName: "Rain_REAL", Method: csvdata.SUM, WindowString: "7h_23h59m59s"},

			// wind
			// wind 7h
			{InputColumnName: "WS_10000_Avg", OutputColumnName: "WS_10m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "WD_10000", OutputColumnName: "WD_10m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "WS_7000_Avg", OutputColumnName: "WS_7m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "WD_7000", OutputColumnName: "WD_7m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "WS_4000_Avg", OutputColumnName: "WS_4m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "WD_4000_Avg", OutputColumnName: "WD_4m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "WS_2000_Avg", OutputColumnName: "WD_2m_07", Method: csvdata.PICK, PickRelative: "7h"},
			// wind 7h30m
			{InputColumnName: "WS_10000_Avg", OutputColumnName: "WS_10m_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "WD_10000", OutputColumnName: "WD_10m_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "WS_7000_Avg", OutputColumnName: "WS_7m_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "WD_7000", OutputColumnName: "WD_7m_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "WS_4000_Avg", OutputColumnName: "WS_4m_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "WD_4000_Avg", OutputColumnName: "WD_4m_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "WS_2000_Avg", OutputColumnName: "WD_2m_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			// wind 13h30m
			{InputColumnName: "WS_10000_Avg", OutputColumnName: "WS_10m_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "WD_10000", OutputColumnName: "WD_10m_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "WS_7000_Avg", OutputColumnName: "WS_7m_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "WD_7000", OutputColumnName: "WD_7m_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "WS_4000_Avg", OutputColumnName: "WS_4m_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "WD_4000_Avg", OutputColumnName: "WD_4m_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "WS_2000_Avg", OutputColumnName: "WD_2m_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			// wind 14h
			{InputColumnName: "WS_10000_Avg", OutputColumnName: "WS_10m_14", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "WD_10000", OutputColumnName: "WD_10m_14", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "WS_7000_Avg", OutputColumnName: "WS_7m_14", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "WD_7000", OutputColumnName: "WD_7m_14", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "WS_4000_Avg", OutputColumnName: "WS_4m_14", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "WD_4000_Avg", OutputColumnName: "WD_4m_14", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "WS_2000_Avg", OutputColumnName: "WD_2m_14", Method: csvdata.PICK, PickRelative: "14h"},
			// wind 17h30m
			{InputColumnName: "WS_10000_Avg", OutputColumnName: "WS_10m_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "WD_10000", OutputColumnName: "WD_10m_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "WS_7000_Avg", OutputColumnName: "WS_7m_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "WD_7000", OutputColumnName: "WD_7m_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "WS_4000_Avg", OutputColumnName: "WS_4m_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "WD_4000_Avg", OutputColumnName: "WD_4m_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "WS_2000_Avg", OutputColumnName: "WD_2m_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			// wind 18h
			{InputColumnName: "WS_10000_Avg", OutputColumnName: "WS_10m_18", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "WD_10000", OutputColumnName: "WD_10m_18", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "WS_7000_Avg", OutputColumnName: "WS_7m_18", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "WD_7000", OutputColumnName: "WD_7m_18", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "WS_4000_Avg", OutputColumnName: "WS_4m_18", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "WD_4000_Avg", OutputColumnName: "WD_4m_18", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "WS_2000_Avg", OutputColumnName: "WD_2m_18", Method: csvdata.PICK, PickRelative: "18h"},
			// wind REALTIME
			{InputColumnName: "WS_10000_Avg", OutputColumnName: "WS_10m_REAL", Method: csvdata.LAST},
			{InputColumnName: "WD_10000", OutputColumnName: "WD_10m_REAL", Method: csvdata.LAST},
			{InputColumnName: "WS_7000_Avg", OutputColumnName: "WS_7m_REAL", Method: csvdata.LAST},
			{InputColumnName: "WD_7000", OutputColumnName: "WD_7m_REAL", Method: csvdata.LAST},
			{InputColumnName: "WS_4000_Avg", OutputColumnName: "WS_4m_REAL", Method: csvdata.LAST},
			{InputColumnName: "WD_4000_Avg", OutputColumnName: "WD_4m_REAL", Method: csvdata.LAST},
			{InputColumnName: "WS_2000_Avg", OutputColumnName: "WD_2m_REAL", Method: csvdata.LAST},

			// temperature
			// temperature 7h
			{InputColumnName: "AT_10000_Avg", OutputColumnName: "TA_10m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "AT_7000_Avg", OutputColumnName: "TA_7m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "AT_4000_Avg", OutputColumnName: "TA_4m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "AT_1200_Avg", OutputColumnName: "TA_1m_07", Method: csvdata.PICK, PickRelative: "7h"},
			// temperature 13h00m
			{InputColumnName: "AT_10000_Avg", OutputColumnName: "TA_10m_1300", Method: csvdata.PICK, PickRelative: "13h"},
			{InputColumnName: "AT_7000_Avg", OutputColumnName: "TA_7m_1300", Method: csvdata.PICK, PickRelative: "13h"},
			{InputColumnName: "AT_4000_Avg", OutputColumnName: "TA_4m_1300", Method: csvdata.PICK, PickRelative: "13h"},
			{InputColumnName: "AT_1200_Avg", OutputColumnName: "TA_1m_1300", Method: csvdata.PICK, PickRelative: "13h"},
			// temperature 18h00m
			{InputColumnName: "AT_10000_Avg", OutputColumnName: "TA_10m_1800", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "AT_7000_Avg", OutputColumnName: "TA_7m_1800", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "AT_4000_Avg", OutputColumnName: "TA_4m_1800", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "AT_1200_Avg", OutputColumnName: "TA_1m_1800", Method: csvdata.PICK, PickRelative: "18h"},
			// temperature REALTIME
			{InputColumnName: "AT_10000_Avg", OutputColumnName: "TA_10m_REAL", Method: csvdata.LAST},
			{InputColumnName: "AT_7000_Avg", OutputColumnName: "TA_7m_REAL", Method: csvdata.LAST},
			{InputColumnName: "AT_4000_Avg", OutputColumnName: "TA_4m_REAL", Method: csvdata.LAST},
			{InputColumnName: "AT_1200_Avg", OutputColumnName: "TA_1m_REAL", Method: csvdata.LAST},
			// temperature MAX
			{InputColumnName: "AT_10000_Avg", OutputColumnName: "TA_10m_MAX", Method: csvdata.MAX, WindowString: "-5h59m59s_18h"},
			{InputColumnName: "AT_7000_Avg", OutputColumnName: "TA_7m_MAX", Method: csvdata.MAX, WindowString: "-5h59m59s_18h"},
			{InputColumnName: "AT_4000_Avg", OutputColumnName: "TA_4m_MAX", Method: csvdata.MAX, WindowString: "-5h59m59s_18h"},
			{InputColumnName: "AT_1200_Avg", OutputColumnName: "TA_1m_MAX", Method: csvdata.MAX, WindowString: "-5h59m59s_18h"},
			// temperature MIN
			{InputColumnName: "AT_10000_Avg", OutputColumnName: "TA_10m_MIN", Method: csvdata.MIN, WindowString: "-9h59m59s_14h"},
			{InputColumnName: "AT_7000_Avg", OutputColumnName: "TA_7m_MIN", Method: csvdata.MIN, WindowString: "-9h59m59s_14h"},
			{InputColumnName: "AT_4000_Avg", OutputColumnName: "TA_4m_MIN", Method: csvdata.MIN, WindowString: "-9h59m59s_14h"},
			{InputColumnName: "AT_1200_Avg", OutputColumnName: "TA_1m_MIN", Method: csvdata.MIN, WindowString: "-9h59m59s_14h"},

			// humidity
			// humidity 7h
			{InputColumnName: "RH_10000_Avg", OutputColumnName: "RH_10m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "RH_7000_Avg", OutputColumnName: "RH_7m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "RH_4000_Avg", OutputColumnName: "RH_4m_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "RH_1200_Avg", OutputColumnName: "RH_1m_07", Method: csvdata.PICK, PickRelative: "7h"},
			// humidity 13h00m
			{InputColumnName: "RH_10000_Avg", OutputColumnName: "RH_10m_1300", Method: csvdata.PICK, PickRelative: "13h"},
			{InputColumnName: "RH_7000_Avg", OutputColumnName: "RH_7m_1300", Method: csvdata.PICK, PickRelative: "13h"},
			{InputColumnName: "RH_4000_Avg", OutputColumnName: "RH_4m_1300", Method: csvdata.PICK, PickRelative: "13h"},
			{InputColumnName: "RH_1200_Avg", OutputColumnName: "RH_1m_1300", Method: csvdata.PICK, PickRelative: "13h"},
			// humidity 18h00m
			{InputColumnName: "RH_10000_Avg", OutputColumnName: "RH_10m_1800", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "RH_7000_Avg", OutputColumnName: "RH_7m_1800", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "RH_4000_Avg", OutputColumnName: "RH_4m_1800", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "RH_1200_Avg", OutputColumnName: "RH_1m_1800", Method: csvdata.PICK, PickRelative: "18h"},
			// humidity REALTIME
			{InputColumnName: "RH_10000_Avg", OutputColumnName: "RH_10m_REAL", Method: csvdata.LAST},
			{InputColumnName: "RH_7000_Avg", OutputColumnName: "RH_7m_REAL", Method: csvdata.LAST},
			{InputColumnName: "RH_4000_Avg", OutputColumnName: "RH_4m_REAL", Method: csvdata.LAST},
			{InputColumnName: "RH_1200_Avg", OutputColumnName: "RH_1m_REAL", Method: csvdata.LAST},
			// humidity MAX
			{InputColumnName: "RH_10000_Avg", OutputColumnName: "RH_10m_MAX", Method: csvdata.MAX, WindowString: "-5h59m59s_18h"},
			{InputColumnName: "RH_7000_Avg", OutputColumnName: "RH_7m_MAX", Method: csvdata.MAX, WindowString: "-5h59m59s_18h"},
			{InputColumnName: "RH_4000_Avg", OutputColumnName: "RH_4m_MAX", Method: csvdata.MAX, WindowString: "-5h59m59s_18h"},
			{InputColumnName: "RH_1200_Avg", OutputColumnName: "RH_1m_MAX", Method: csvdata.MAX, WindowString: "-5h59m59s_18h"},
			// humidity MIN
			{InputColumnName: "RH_10000_Avg", OutputColumnName: "RH_10m_MIN", Method: csvdata.MIN, WindowString: "-9h59m59s_14h"},
			{InputColumnName: "RH_7000_Avg", OutputColumnName: "RH_7m_MIN", Method: csvdata.MIN, WindowString: "-9h59m59s_14h"},
			{InputColumnName: "RH_4000_Avg", OutputColumnName: "RH_4m_MIN", Method: csvdata.MIN, WindowString: "-9h59m59s_14h"},
			{InputColumnName: "RH_1200_Avg", OutputColumnName: "RH_1m_MIN", Method: csvdata.MIN, WindowString: "-9h59m59s_14h"},

			// evaporation
			// TODO water level

			// water temperature
			{InputColumnName: "WT_Avg", OutputColumnName: "WT_07", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "WT_Avg", OutputColumnName: "WT_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "WT_Avg", OutputColumnName: "WT_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "WT_Avg", OutputColumnName: "WT_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "WT_Avg", OutputColumnName: "WT_REAL", Method: csvdata.LAST},

			// wind evaporation
			{InputColumnName: "WS_500_Avg", OutputColumnName: "WS_500cm_0700", Method: csvdata.PICK, PickRelative: "7h"},
			{InputColumnName: "WS_500_Avg", OutputColumnName: "WS_500cm_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "WS_500_Avg", OutputColumnName: "WS_500cm_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "WS_500_Avg", OutputColumnName: "WS_500cm_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "WS_500_Avg", OutputColumnName: "WS_500cm_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "WS_500_Avg", OutputColumnName: "WS_500cm_1800", Method: csvdata.PICK, PickRelative: "18h"},
			{InputColumnName: "WS_500_Avg", OutputColumnName: "WS_500cm_REAL", Method: csvdata.LAST},

			// Soil temperature Turf 0, 20, 50, 100, 200, 300, 400, 500, 600, 750, 1000 cm
			// soil temperature 7h30m
			{InputColumnName: "STA_0_Avg", OutputColumnName: "ST1_0_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "STA_20_Avg", OutputColumnName: "ST1_20_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST1_200_Avg", OutputColumnName: "ST1_200_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST1_300_Avg", OutputColumnName: "ST1_300_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST1_400_Avg", OutputColumnName: "ST1_400_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST1_500_Avg", OutputColumnName: "ST1_500_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST1_600_Avg", OutputColumnName: "ST1_600_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST1_750_Avg", OutputColumnName: "ST1_750_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST1_1000_Avg", OutputColumnName: "ST1_1000_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			// soil temperature 13h30m
			{InputColumnName: "STA_0_Avg", OutputColumnName: "ST1_0_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "STA_20_Avg", OutputColumnName: "ST1_20_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST1_50_Avg", OutputColumnName: "ST1_50_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST1_100_Avg", OutputColumnName: "ST1_100_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST1_200_Avg", OutputColumnName: "ST1_200_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST1_300_Avg", OutputColumnName: "ST1_300_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST1_400_Avg", OutputColumnName: "ST1_400_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST1_500_Avg", OutputColumnName: "ST1_500_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST1_600_Avg", OutputColumnName: "ST1_600_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST1_750_Avg", OutputColumnName: "ST1_750_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST1_1000_Avg", OutputColumnName: "ST1_1000_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			// soil temperature 14h00m
			{InputColumnName: "STA_0_Avg", OutputColumnName: "ST1_0_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "STA_20_Avg", OutputColumnName: "ST1_20_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST1_50_Avg", OutputColumnName: "ST1_50_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST1_100_Avg", OutputColumnName: "ST1_100_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST1_200_Avg", OutputColumnName: "ST1_200_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST1_300_Avg", OutputColumnName: "ST1_300_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST1_400_Avg", OutputColumnName: "ST1_400_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST1_500_Avg", OutputColumnName: "ST1_500_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST1_600_Avg", OutputColumnName: "ST1_600_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST1_750_Avg", OutputColumnName: "ST1_750_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST1_1000_Avg", OutputColumnName: "ST1_1000_1400", Method: csvdata.PICK, PickRelative: "14h"},
			// soil temperature 17h30m
			{InputColumnName: "STA_0_Avg", OutputColumnName: "ST1_0_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "STA_20_Avg", OutputColumnName: "ST1_20_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST1_50_Avg", OutputColumnName: "ST1_50_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST1_100_Avg", OutputColumnName: "ST1_100_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST1_200_Avg", OutputColumnName: "ST1_200_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST1_300_Avg", OutputColumnName: "ST1_300_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST1_400_Avg", OutputColumnName: "ST1_400_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST1_500_Avg", OutputColumnName: "ST1_500_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST1_600_Avg", OutputColumnName: "ST1_600_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST1_750_Avg", OutputColumnName: "ST1_750_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST1_1000_Avg", OutputColumnName: "ST1_1000_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			// soil temperature REALTIME
			{InputColumnName: "STA_0_Avg", OutputColumnName: "ST1_0_REAL", Method: csvdata.LAST},
			{InputColumnName: "STA_20_Avg", OutputColumnName: "ST1_20_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST1_50_Avg", OutputColumnName: "ST1_50_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST1_100_Avg", OutputColumnName: "ST1_100_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST1_200_Avg", OutputColumnName: "ST1_200_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST1_300_Avg", OutputColumnName: "ST1_300_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST1_400_Avg", OutputColumnName: "ST1_400_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST1_500_Avg", OutputColumnName: "ST1_500_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST1_600_Avg", OutputColumnName: "ST1_600_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST1_750_Avg", OutputColumnName: "ST1_750_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST1_1000_Avg", OutputColumnName: "ST1_1000_REAL", Method: csvdata.LAST},

			// Soil temperature bare 0, 20, 50, 100, 200, 300, 400, 500, 600, 750, 1000 cm
			// soil temperature 7h30m
			{InputColumnName: "STB_0_Avg", OutputColumnName: "ST2_0_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "STB_20_Avg", OutputColumnName: "ST2_20_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST2_200_Avg", OutputColumnName: "ST2_200_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST2_300_Avg", OutputColumnName: "ST2_300_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST2_400_Avg", OutputColumnName: "ST2_400_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST2_500_Avg", OutputColumnName: "ST2_500_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST2_600_Avg", OutputColumnName: "ST2_600_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST2_750_Avg", OutputColumnName: "ST2_750_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			{InputColumnName: "ST2_1000_Avg", OutputColumnName: "ST2_1000_0730", Method: csvdata.PICK, PickRelative: "7h30m"},
			// soil temperature 13h30m
			{InputColumnName: "STB_0_Avg", OutputColumnName: "ST2_0_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "STB_20_Avg", OutputColumnName: "ST2_20_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST2_50_Avg", OutputColumnName: "ST2_50_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST2_100_Avg", OutputColumnName: "ST2_100_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST2_200_Avg", OutputColumnName: "ST2_200_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST2_300_Avg", OutputColumnName: "ST2_300_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST2_400_Avg", OutputColumnName: "ST2_400_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST2_500_Avg", OutputColumnName: "ST2_500_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST2_600_Avg", OutputColumnName: "ST2_600_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST2_750_Avg", OutputColumnName: "ST2_750_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			{InputColumnName: "ST2_1000_Avg", OutputColumnName: "ST2_1000_1330", Method: csvdata.PICK, PickRelative: "13h30m"},
			// soil temperature 14h00m
			{InputColumnName: "STB_0_Avg", OutputColumnName: "ST2_0_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "STB_20_Avg", OutputColumnName: "ST2_20_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST2_50_Avg", OutputColumnName: "ST2_50_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST2_100_Avg", OutputColumnName: "ST2_100_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST2_200_Avg", OutputColumnName: "ST2_200_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST2_300_Avg", OutputColumnName: "ST2_300_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST2_400_Avg", OutputColumnName: "ST2_400_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST2_500_Avg", OutputColumnName: "ST2_500_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST2_600_Avg", OutputColumnName: "ST2_600_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST2_750_Avg", OutputColumnName: "ST2_750_1400", Method: csvdata.PICK, PickRelative: "14h"},
			{InputColumnName: "ST2_1000_Avg", OutputColumnName: "ST2_1000_1400", Method: csvdata.PICK, PickRelative: "14h"},
			// soil temperature 17h30m
			{InputColumnName: "STB_0_Avg", OutputColumnName: "ST2_0_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "STB_20_Avg", OutputColumnName: "ST2_20_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST2_50_Avg", OutputColumnName: "ST2_50_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST2_100_Avg", OutputColumnName: "ST2_100_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST2_200_Avg", OutputColumnName: "ST2_200_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST2_300_Avg", OutputColumnName: "ST2_300_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST2_400_Avg", OutputColumnName: "ST2_400_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST2_500_Avg", OutputColumnName: "ST2_500_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST2_600_Avg", OutputColumnName: "ST2_600_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST2_750_Avg", OutputColumnName: "ST2_750_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			{InputColumnName: "ST2_1000_Avg", OutputColumnName: "ST2_1000_1730", Method: csvdata.PICK, PickRelative: "17h30m"},
			// soil temperature REALTIME
			{InputColumnName: "STB_0_Avg", OutputColumnName: "ST2_0_REAL", Method: csvdata.LAST},
			{InputColumnName: "STB_20_Avg", OutputColumnName: "ST2_20_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST2_50_Avg", OutputColumnName: "ST2_50_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST2_100_Avg", OutputColumnName: "ST2_100_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST2_200_Avg", OutputColumnName: "ST2_200_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST2_300_Avg", OutputColumnName: "ST2_300_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST2_400_Avg", OutputColumnName: "ST2_400_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST2_500_Avg", OutputColumnName: "ST2_500_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST2_600_Avg", OutputColumnName: "ST2_600_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST2_750_Avg", OutputColumnName: "ST2_750_REAL", Method: csvdata.LAST},
			{InputColumnName: "ST2_1000_Avg", OutputColumnName: "ST2_1000_REAL", Method: csvdata.LAST},
		},
		TimeOffset:    "6h30m", //"6h30m",
		StartTime:     time.Date(2023, 11, 3, 0, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2023, 11, 3, 0, 0, 0, 0, time.UTC),
		TimePrecision: "second",
		AggWindow:     "24h", //"24h"
	}

	result, err := csvdata.CsvAggregateTable(cfg)
	if err != nil {
		// Print the error
		fmt.Println("Error: ", err)
		return
	}

	// Print elapsed time
	fmt.Println("Elapsed time: ", time.Now().UTC().UnixMilli()-starttime, "ms")

	// Print the result
	fmt.Println("Result:")
	jsonout, _ := result.ToJson5()
	fmt.Println(string(jsonout))

}
