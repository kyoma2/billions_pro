
from strategy.stghunter import *
from masquerade.rader import *
from conn.invate import *

class backupstg:
    def __init__(self):
        self.cur_timestp = ""

    def run(self):
        net_income = net_moneyflow(df,self.cur_timestp)
        jump_open = jump_open_stg(df, self.cur_timestp)
        fake_2reds = fake_2reds_stg(df,self.cur_timestp)



        three_star = masquerade(df,[  crossing_star(),
                                         crossing_star(),
                                        (crossing_star(color="red"),breakdown_ma())]
                                    )


        jump_gre = masquerade(df,[common(increase_per=(-9.8,-1),red=False),
                                    crossing_star(),
                                    common(increase_per=(0,2),jump=(2.5,5),red= False)
                                    ])

        quick_up_down = masquerade(df,[common(increase_per=(9.8,10)),
                              common(increase_per=(9.8, 10),jump=(1,10)),
                              common(increase_per=(-10, -5)),
                              common(increase_per=(-10, -1))
                                    ])


        fake_2gre = masquerade(df, [common(increase_per=(5, 11), red=True),
                                    common(increase_per=(0, 10), red=False),
                                    common(increase_per=(0, 10), red=False),
                                    ])
        #
        fake_1reds = masquerade(df,[common( red=True),
                                    [common(increase_per=(-10, -0.3), red=True), crossing_star()]
                                    ])


        raise_after_ma = masquerade(df,[breakdown_ma(on=('ma_m3'))
                                    ,common(increase_per=(4,11))
                                    ])

        macd_120 = macd_120_stg(df,self.cur_timestp,default="macd_120")
        one_shoot = long_head_stg(df,self.cur_timestp)
        kdj = kdj_stg(df, self.cur_timestp)
        macd = macd_stg(df, self.cur_timestp, default="macd")