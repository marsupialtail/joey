v_conditions = [
    ('a', "a.is_local_top"),
    ('b', "b.is_local_bottom and b.close < a.close * LOWER"),
    ('c', "c.close > a.close")
]

ascending_triangles_conditions =  [('a', "a.is_local_bottom"), # first bottom 
     ('b', """b.is_local_top and b.close > a.close * UPPER"""), # first top
     ('c', """c.is_local_bottom and c.close < b.close * LOWER and c.close > a.close * UPPER"""), # second bottom, must be higher than first bottom
     ('d', """d.is_local_top and d.close > c.close * UPPER and abs(d.close / b.close) < UPPER"""), # second top, must be similar to first top
     ('e', """e.is_local_bottom and e.close < d.close * LOWER and e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third bottom, didn't break support
     ('f', """f.close > d.close * UPPER""") #breakout resistance
]

descending_triangles_conditions = [('a', "a.is_local_top"), # first top
        ('b', """b.is_local_bottom and b.close < a.close * LOWER"""), # first bottom
        ('c', """c.is_local_top and c.close > b.close * UPPER and c.close < a.close * LOWER"""), # second top, must be lower than first top
        ('d', """d.is_local_bottom and d.close < c.close * LOWER and abs(d.close / b.close) < UPPER"""), # second bottom, must be similar to first bottom
        ('e', """e.is_local_top and e.close > d.close * UPPER and e.close < (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third top, didn't break resistance
        ('f', """f.close < d.close * LOWER""") #breakout support
    ]

symmetrical_triangles_conditions_1 = [('a', "a.is_local_top"), # first top
        ('b', """b.is_local_bottom and b.close < a.close * LOWER"""), # first bottom
        ('c', """c.is_local_top and c.close > b.close * UPPER and c.close < a.close * LOWER"""), # second top, must be lower than first top
        ('d', """d.is_local_bottom and d.close < c.close * LOWER and d.close > b.close * UPPER"""), # second bottom, must be similar to first bottom
        ('e', """e.is_local_top and e.close > d.close * UPPER and e.close < (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third top, didn't break resistance
        ('f', """f.close < d.close * LOWER""") #breakout support
    ]

symmetrical_triangles_conditions_2 = [('a', "a.is_local_bottom"), # first bottom
        ('b', """b.is_local_top and b.close > a.close * UPPER"""), # first top
        ('c', """c.is_local_bottom and c.close < b.close * LOWER and c.close > a.close * UPPER"""), # second bottom, must be higher than first bottom
        ('d', """d.is_local_top and d.close > c.close * UPPER and d.close < b.close * LOWER"""), # second top, must be similar to first top
        ('e', """e.is_local_bottom and e.close < d.close * LOWER and e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third bottom, didn't break support
        ('f', """f.close > d.close * UPPER""") #breakout resistance
    ]

flag_1 = [('a', "a.is_local_top"), # first top
        ('b', """b.is_local_bottom and b.close < a.close * LOWER"""), # first bottom
        ('c', """c.is_local_top and c.close > b.close * UPPER"""), # second top, must be lower than first top
        ('d', """d.is_local_bottom and d.close < c.close * LOWER and d.close > b.close * UPPER"""), # second bottom, must be similar to first bottom
        ('e', """e.is_local_top and e.close > d.close * UPPER and e.close < (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third top, didn't break resistance
        ('f', """f.close < d.close * LOWER""") #breakout support
    ]

heads_and_shoulders_conditions = [('a', "a.is_local_top"), # first shoulder
        ('b', """b.is_local_bottom and b.close < a.close * LOWER"""), # first bottom
        ('c', "c.is_local_top and c.close > a.close * UPPER"), # head
        ('d', "d.is_local_bottom and d.close < a.close * LOWER"), # second bottom
        ("e", "e.is_local_top and e.close > d.close * UPPER and e.close < c.close * LOWER"), # second shoulder
        ("f", "f.close < ((d.close - b.close) / (d.timestamp - b.timestamp) * (f.timestamp - b.timestamp) + b.close) * LOWER"), # neckline
    ]

cup_and_handle_conditions = [
    ('a', 'a.is_local_top'),
    ('b', 'b.is_local_bottom and b.close < a.close * LOWER'), #cup low
    ('c', 'c.is_local_top and c.close > b.close * UPPER'), 
    ('d', 'd.is_local_bottom and d.close < c.close * LOWER and d.close > b.close * UPPER'), #handle low must be higher than cup low
    ('e', 'e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close  and (e.timestamp - c.timestamp) < (c.timestamp - a.timestamp) * 0.6'), #breakout
]

udf_cup_and_handle = [
    ('a', 'a.is_local_top'),
    ('b', 'b.is_local_bottom and b.close < a.close * LOWER'), #cup low
    ('c', 'c.is_local_top and c.close > b.close * UPPER'), 
    ('d', 'd.is_local_bottom and d.close < c.close * LOWER and d.close > b.close * UPPER'), #handle low must be higher than cup low
    ('e', 'e.close > lin_reg(a.close, a.timestamp, c.close, c.timestamp, e.timestamp)  and (e.timestamp - c.timestamp) < (c.timestamp - a.timestamp) * 0.6'), #breakout
]

def lin_reg(a_close, a_timestamp, c_close, c_timestamp, e_timestamp):
    return (c_close - a_close) / (c_timestamp - a_timestamp) * (e_timestamp - a_timestamp) + a_close 