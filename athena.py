
# heitao Spades
# meihua Club
# hongtao Heart
# fangpian Diamon
import re
import copy
from random import shuffle


class card():
    def __init__(self,fullname,opened,movable,inq, curq=False):
        self.fullname = fullname
        self.opened = opened
        self.movable = movable
        self.inq = inq  #if this card in queue
        self.curq = curq # if the card is current card in queue

        if fullname == 'UNK':
            self.kind = 'unk'
            self.number = 'unk'
            self.color = 'unk'
            self.inq = 'unk'
            return

        reg = re.match(r"([CSHD])(\d+)",fullname)



        self.kind = reg.group(1)
        self.number = int(reg.group(2))

        if fullname[0] == 'C': #club: black
            self.kind='Club'
            self.color = 'black'
        elif fullname[0] == 'S': #spade black
            self.kind = 'Spade'
            self.color = 'black'
        elif fullname[0] == 'H': #heart red
            self.kind = 'Heart'
            self.color = 'red'
        elif fullname[0] == 'D': #diamond red
            self.kind = 'Diamon'
            self.color = 'red'
        else:
            print("unknow card kind "+str(fullname))
            exit(0)



class card_in_table:
    def __init__(self):
        self.row = 0
        self.col = 0
        self.card = 0
        self.opened = 0


############# INIT START ###########
#### Decks
deck={}
deck['Spade']=[card(fullname='S0',opened=True,movable=False,inq=False)]
deck['Club']=[card(fullname='C0',opened=True,movable=False,inq=False)]
deck['Heart']=[card(fullname='H0',opened=True,movable=False,inq=False)]
deck['Diamon']=[card(fullname='D0',opened=True,movable=False,inq=False)]


#### Queue in list
queue = [
    # 'H5', #debug
    'C7','C9','D6','H7','D2',
    'D10','S10','D12','D11','S11',
    'D3','S1','D5','C5','D1',
    'D13','H8','H13','C10','C11',
    'C4','H2','S7','H5'
         ]


# col from left to right
# in a column, card from bottom to top. row[0] if the bottom card.
col=[]
desk_card=[]

for i in range(7):
    col.append([])
    desk_card.append([])

##### Desk COLS
col[0]=['D7','H1','D8','UNK']
col[1]=['C12','UNK','S4','UNK']
col[2]=['C6','H6','C2','D9']
col[3]=['S6','S9','S5','S12']
col[4]=['S3','H10','H3','UNK']
col[5]=['H11','H12','C8','D4']
col[6]=['C1','C3','S2','C13']






queue_card = []
for c in queue:
    _tmp = card(fullname=c,opened=True,movable=False,inq=True)
    queue_card.append(_tmp)





for c in range(7):
    i=1 #1: the bottom layer,opened.   2:closed, 3:opened, 4:closed
    for fn in col[c]:
        if i==1:
            _tmp = card(fullname=fn, opened=True, movable=True,inq=False)
        if i==3:
            _tmp = card(fullname=fn, opened=True, movable=False,inq=False)
        elif i==2 or i==4:
            _tmp = card(fullname=fn, opened=False,movable=False,inq=False)
        i+=1
        desk_card[c].append(_tmp)



def print_a_card(c):
    print(f"{c.fullname}_{c.color[0]}_O{str(c.opened)[0]}_M{str(c.movable)[0]}\t",end="")


def print_q():
    global queue_card
    for i in reversed(range(queue_card.__len__())):
        c=queue_card[i]
        if not c.inq:
            continue
        if c.curq:
            print(f"{c.fullname}_{c.color[0]}* ",end="")
        else:
            print(f"{c.fullname}_{c.color[0]} ",end="")
    print()



def print_desk():
    global desk_card

    len_max = 0
    len_col_left = []
    for col in range(7):
        len_col_left.append(desk_card[col].__len__())
        len_max = desk_card[col].__len__() if len_max < desk_card[col].__len__() else len_max

    # print(f"max len match {len_max}")

    for row in reversed(range(len_max)):
        for col in range(7):

            if len_col_left[col] <=0 :
                print(f"          \t",end="")
                continue

            c = desk_card[col][len_col_left[col]-1]
            print_a_card(c)
            len_col_left[col] -= 1
        print("")

def print_deck():
    global deck

    # kind = ['Spade','Heart','Club','Diamon']
    len_max = 0
    ken_len_left={}

    for k in deck.keys():
        ken_len_left[k]=deck[k].__len__()
        len_max = deck[k].__len__() if len_max < deck[k].__len__() else len_max

    # print(f"max len match {len_max}")

    for row in reversed(range(len_max)):
        for k in deck.keys():
            if ken_len_left[k] <= 0 :
                print(f"   \t",end="")
                continue

            c = deck[k][ken_len_left[k]-1]
            print(f"{c.fullname}_{c.color[0]}\t", end="")
            ken_len_left[k] -= 1
        print("")

########### INIT END ##############
def pa():
    print("---"*9)
    print_deck()

    print("---"*3)
    print_q()

    print("---"*3)
    print_desk()
    print("---" * 9)


######## MOVE LOGIC START###############
def get_above_card(col,row):
    global desk_card
    global queue_card
    global desk

    if row+1 < desk_card[col].__len__(): #should not <=
        ac_row = row + 1
        return(True, col, ac_row, desk_card[col][ac_row])
    else:
        print("no above card")
        return(False, None, None, None)


def open_card(col,row):
    global desk_card
    global queue_card
    global desk

    card = desk_card[col][row]
    card.opened=True
    card.movable=True
    print(f"OPEN card at col {col}, row {row} {card.fullname}")


    if card.fullname == 'UNK':
        print(f"Riched UNK card, stop and review previous moving steps !!!")
        exit()


def move_cards_from_col_to_another_col(frm_c,frm_r, t_c):
    global desk_card
    global queue_card
    global desk

    #open above card. MUST do before moving card.
    has_ac, ac_col, ac_row, ac_card = get_above_card(frm_c,frm_r)
    if has_ac:
        open_card(col=ac_col,row=ac_row)

    #cut cards from
    list_be_moved = desk_card[frm_c][:frm_r+1]
    print(f"cards to MOVE {str(list_be_moved.__len__())}")

    desk_card[frm_c] = desk_card[frm_c][frm_r+1:]

    #set cards be moved not movable, avoid pingpong
    for _ in iter(list_be_moved):
        _.movable=False

    #append cards to
    desk_card[t_c] = list_be_moved + desk_card[t_c]
    desk_card[t_c][frm_r+1].movable=True



#### SCAN logic start
def search_in_desk(color,number):
    global desk_card
    global queue_card
    global desk

    sr = [_ for _ in range(7)]
    shuffle(sr)
    for col in sr:
        for row in reversed(range(desk_card[col].__len__())):
            c = desk_card[col][row]
            # print(f"col {col} row {row}  {c.fullname} {c.color} {str(c.opened)} {str(c.movable)}")

            if c.color == color and c.number==number:


                if c.movable:
                    print(f"found the card on desk and movable. {c.fullname} at C{str(col)}R{str(row)}")
                    return (True, col, row, c)
                else:
                    # print(f"found the card on desk but unmovable.")
                    continue
    # print(f"not found the card in desk ")
    return(False,None,None,None)


def scan_bottom():
    global desk_card
    global queue_card
    global desk

    card_moved_this_round = False

    sr = [_ for _ in range(7)]
    shuffle(sr)
    for c in sr:
        if desk_card[c].__len__() == 0:
            continue

        to_crd = desk_card[c][0]
        exp_color = "black" if to_crd.color=="red" else "red"
        exp_number = to_crd.number - 1

        # print(f"col {str(c)} looking for {exp_color}{str(exp_number)}")

        # if exp_color=='red' and exp_number==6:
        #     a=1
        #     print("debug stop")

        (rc,frm_c,frm_r,frm_crd) = search_in_desk(color=exp_color,number=exp_number)

        if rc==True:
            card_moved_this_round = True
            t_r = 0
            print(f"MOVE column, {frm_crd.fullname} to {to_crd.fullname}, C{str(frm_c)}R{str(frm_r)} -> C{str(c)}R{str(t_r)}")
            move_cards_from_col_to_another_col(frm_c=frm_c, frm_r=frm_r, t_c=c)

    if card_moved_this_round:
        scan_bottom() #recursive. scan the desk WHENEVER card moved.


def desk_to_deck():
    global desk_card
    global queue_card
    global desk

    sr = [_ for _ in range(7)]
    shuffle(sr)
    for c in sr:
        if desk_card[c].__len__() == 0:
            continue

        frm_crd = desk_card[c][0]

        exp_kind =  frm_crd.kind
        exp_number = frm_crd.number - 1

        deck_card = deck[exp_kind][-1]

        if deck_card.number == exp_number:
            print(f"MOVE to deck, {frm_crd.fullname} C{str(c)}R{str(0)}")

            has_ac, ac_col, ac_row, ac_card = get_above_card(c, 0)
            if not has_ac:
                print("cannot open card, no above card.")
                return

            open_card(col=c, row=1)
            deck[exp_kind].append(frm_crd)
            desk_card[c].remove(frm_crd)
            desk_to_deck()


def a_queue_card_to_desk():
    global desk_card
    global queue_card
    global desk

    crd = get_cur_qcard()
    q_prev, q_next = get_qc_nebour(crd)

    # prev_q_cur_card = None
    # for _i in range(queue_card.__len__()):
    #     if _i>0 and queue_card[_i].fullname==crd.fullname:
    #         prev_q_cur_card = queue_card[_i-1]

    exp_color = "black" if crd.color == 'red' else 'red'
    exp_num = crd.number+1

    sr = [_ for _ in range(7)]
    shuffle(sr)
    for col in sr:
        if desk_card[col].__len__()==0:
            if crd.number==12:
                print(f"MOVE K to empty desk col {str(col)}, {crd.fullname}")

                desk_card[col].append(crd) #insert to desk

                crd.inq = False #remove from q
                crd.curq = False

                if q_prev != None: #assign next qc
                    q_prev.curq = True
                elif q_next != None:
                    q_next.curq = True
                else:
                    print("ERROR, not able to determin current qc")
                continue
            else:
                continue

        if desk_card[col][0].color == exp_color and desk_card[col][0].number == exp_num:
            print(f"MOVE from queue to desk, {crd.fullname} -> {desk_card[col][0].fullname}, C{str(col)}R0")
            # inser to desk
            crd.inq = False
            crd.movable = True
            crd.opened = True
            crd.curq = False

            desk_card[col] = [crd] + desk_card[col]

            if q_prev != None:
                q_prev.curq = True
            elif q_next != None:
                q_next.curq = True
            else:
                print("ERROR, not able to determin current qc")


            #previous queue card is available now.
            if not a_queue_card_to_desk():
                a_queue_card_to_deck()
            return(True)
    # print(f"card in queue {crd.fullname} has no place in desk columns")
    return(False)


def a_queue_card_to_deck():
    global desk_card
    global queue_card
    global desk

    crd = get_cur_qcard()
    q_prev, q_next = get_qc_nebour(crd)

    # prev_q_cur_card = None
    # for _i in range(queue_card.__len__()):
    #     if _i>0 and queue_card[_i].fullname==crd.fullname:
    #         prev_q_cur_card = queue_card[_i-1]

    exp_kind = crd.kind
    exp_number = crd.number - 1

    deck_card = deck[exp_kind][-1]
    if deck_card.number == exp_number:
        print(f"MOVE from queue to deck, {crd.fullname}")
        crd.inq = False
        crd.movable = True
        crd.opened = True
        crd.curq = False
        deck[exp_kind].append(crd)

        if q_prev != None:
            q_prev.curq = True
        elif q_next != None:
            q_next.curq = True

        # previous queue card is available now.
        if not a_queue_card_to_deck():
            a_queue_card_to_desk()

        return(True)

    return(False)



def get_q_length():
    ql = 0
    for i in range(queue_card.__len__()):
        if queue_card[i].inq:
            ql+=1
    return(ql)

def get_qc_nebour(crd):
    global queue_card

    prev = None
    next = None


    real_q = []
    for i in range(queue_card.__len__()):
        c = queue_card[i]
        if not c.inq:
            continue
        else:
            real_q.append(c)


    # if crd.fullname == 'C4':
    #     print('pause')

    for i in range(real_q.__len__()):
        c = real_q[i]
        # print(c.fullname)
        if c.fullname == crd.fullname:
            if i-1 > 0:
                prev = real_q[i-1]
            if i+1 < real_q.__len__():
                next = real_q[i+1]

    return(prev,next)


def clear_curq():
    global queue_card
    for i in range(queue_card.__len__()):
        c = queue_card[i]
        if c.curq:
            c.curq=False
            return

def get_cur_qcard():
    global queue_card
    r_list = []
    cq_idx = 0

    p_crd_in_q=None
    for i in range(queue_card.__len__()):
        c = queue_card[i]
        if c.curq:
            # print(f"current queue card {str(c.fullname)}, idx {str(i)}")
            r_list.append(c)

    if r_list.__len__()>1:
        print(f"ERROR, current card in q should not more than 1, {str(r_list.__len__())}")
        for _ in iter(r_list):
            print(_.fullname)
        exit(1)

    if r_list.__len__()==0:
        print("WARN, current card in q is empty")
        for _ in iter(r_list):
            print(_.fullname)

    return(r_list[0])


def transvers_queue():
    global desk_card
    global queue_card
    global desk

    print(f"begin queue length {str(get_q_length())}")

    for i in range(queue_card.__len__()):
        c = queue_card[i]

        if not c.inq: #card no longer in q.
            continue

        clear_curq()
        c.curq = True
        # print("MMM"+c.fullname)
        # print_cur_qcard()
        # print("---")

        sr = [_ for _ in range(4)]
        shuffle(sr)
        for _t in sr:
            if _t==0:
                scan_bottom()
            elif _t == 1:
                desk_to_deck()
            elif _t == 2:
                a_queue_card_to_desk()
            elif _t == 3:
                a_queue_card_to_deck()
        c.curq = False
        # print("MMME"+c.fullname)
        # if c.fullname == 'H7':
        #     print("pause")


    pa()
    print(f"after queue length {str(get_q_length())}\n")



# def check_desk():
#     global desk_card
#     global queue_card
#     global desk
#
#     for i in range(7):
#         col = desk_card[i]
#         for j in range(col.__len__()):
#             c =col[j]
#
#


##### MAIN, PLAY NOW #####
# pa()
transvers_queue()
transvers_queue()
transvers_queue()
#
transvers_queue()
transvers_queue()

transvers_queue()
# scan_bottom()
# desk_to_deck()