FIRST = [
    'F123L', 'F122L', 'F111L', 'F112L', 'F113L', 'F133L', 'F223L', 'F233L', 'F333L',
    'F11LL', 'F12LL', 'F13LL', 'F22LL', 'F23LL', 'F33LL', 'FLLLL', 'FF13L', 'FF12L',
    'FF11L', 'FF23L', 'FF22L', 'FF33L', 'FF1LL', 'FF2LL', 'FF3LL', 'FFLLL', 'FFF1L',
    'FFF2L', 'FFF3L', 'FFFLL', 'FFF11', 'FFF12', 'FFF13', 'FFF22', 'FFF23', 'FFF33',
    'FFFFL', 'FFFF1', 'FFFF2', 'FFFF3', 'FF123', 'FF122', 'FF113', 'FF112', 'FF111',
    'FF133', 'FF233', 'FF223', 'FF222', 'FF333', 'F1233', 'F1223', 'F1123', 'F1113',
    'F1112', 'F1122', 'F1222', 'F1133', 'F1333', 'F2333', 'F2233', 'F2223', 'F2222',
    'F3333', 'F1111', 'LLLLL', '1LLLL', '11LLL', '111LL', '1111L', '12LLL', '122LL',
    '1222L', '123LL', '1233L', '13LLL', '133LL', '1333L', '2LLLL', '22LLL', '222LL',
    '2222L', '23LLL', '233LL', '2333L', '3LLLL', '33LLL', '333LL', '3333L'
]

SECOND = [
    'F11L', 'F12L', 'F13L', 'F22L', 'F23L', 'F33L', 'FF1L', 'FF2L', 'FF3L', 'FLLL',
    'FFLL', 'FFFL', 'F1LL', 'F2LL', 'F3LL'
]

THIRD = [
    'F1L', 'F2L', 'F3L', 'FLL', 'FFL', 'FF1', 'FF2', 'FF3', 'F11', 'F12',
    'F13', 'F22', 'F23', 'F33'
]

FOURTH = [
    'F12XL', 'F11XL', 'F13XL', 'F22XL', 'F23XL', 'F33XL', 'F1XLL', 'F2XLL', 'F3XLL',
    'FXLLL', 'FX13L', 'FX12L', 'FX11L', 'FX23L', 'FX22L', 'FX33L', 'FX1LL', 'FX2LL',
    'FX3LL', 'FF1XL', 'FF2XL', 'FF3XL', 'FFXLL', 'FFX1L', 'FFX2L', 'FFX3L', 'FFFXL',
    'FFFFX', 'FFF1X', 'FFF2X', 'FFF3X', 'FF11X', 'FF12X', 'FF13X', 'FF22X', 'FF23X',
    'FF33X', 'F123X', 'F111X', 'F112X', 'F113X', 'F122X', 'F133X', 'F222X', 'F223X',
    'F233X', 'F333X'
]

FITFTH = [
    'F1LX', 'F2LX', 'F3LX', 'F12X', 'F13X', 'F11X', 'F23X', 'F22X', 'F33X', 'FFLX',
    'FF1X', 'FF2X', 'FF3X', 'FFFX', '123X', '122X', '111X', '223X', '233X', '333X',
    'LLLX', 'F1XL', 'F2XL', 'F3XL', 'FFXL', 'F1X2', 'F1X3', 'F2X3', 'FFX1', 'FFX2',
    'FFX3', 'FX1L', 'FX2L', 'FX3L', 'FXLL', 'FX12', 'FX13', 'FX11', 'FX23', 'FX22',
    'FX33'
]

SIXTH = [
    'F1111L', 'F1112L', 'F1113L', 'F1122L', 'F1123L', 'F1133L', 'F1222L', 'F1223L',
    'F1233L', 'F2222L', 'F2223L', 'F2233L', 'F2333L', 'F3333L', 'FF111L', 'FF112L',
    'FF113L', 'FF122L', 'FF123L', 'FF222L', 'FF223L', 'FF233L', 'FF333L', 'FFF11L',
    'FFF12L', 'FFF13L', 'FFF22L', 'FFF23L', 'FFF33L', 'FFFF1L', 'FFFF2L', 'FFFF3L',
    'F111LL', 'F112LL', 'F113LL', 'F122LL', 'F123LL', 'F133LL', 'F222LL', 'F223LL',
    'F233LL', 'F333LL', 'F11LLL', 'F12LLL', 'F13LLL', 'F22LLL', 'F23LLL', 'F33LLL',
    'F1LLLL', 'F2LLLL', 'F3LLLL', 'FLLLLL', 'FFLLLL', 'FFFLLL', 'FFFFLL', 'FFFFFL'
]


def _readable_combi(combi):
    readable_combi = []
    max_position = -1
    for i in range(0, len(combi)):
        # First name
        if combi[i] == 'F':
            position = 0
        # First middle name
        if combi[i] == '1':
            position = 1
        # Second middle name
        if combi[i] == '2':
            position = 2
        # Third middle name
        if combi[i] == '3':
            position = 3
        # Last name (independant of middle names)
        if combi[i] == 'L':
            position = -1
        if combi[i] == 'X':
            position = None
        if position is not None and position > max_position:
            max_position = position
        readable_combi.append(position)
    return (readable_combi, max_position)
