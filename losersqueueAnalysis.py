import matplotlib.pyplot as plt
import pandas as pd
import datetime
from scipy.stats import spearmanr
from numpy import polyfit

# Use on ordinal summed data to convert to individual observations at their indices
# e.g. [2, 3, 4] -> [1, 1, 2, 2, 2, 3, 3, 3, 3]
def repeatListByList(listToRepeat, numRepeatsList):
    repeatedList = []
    elementIndex = 0
    for element, repeats in zip(listToRepeat, numRepeatsList):
        newElements = [element] * int(repeats)
        repeatedList = repeatedList + newElements
    return repeatedList


unrolledDfFile = 'unrolledDf.parquet'
personName = 'your'
streakTime = datetime.timedelta(minutes=30)


matchDf = pd.read_parquet(unrolledDfFile, engine='fastparquet')


# The last game doesn't have a followup so we ignore it in the stats
matchDf.drop(matchDf.iloc[-1].name, inplace=True)
numGames = float(len(list(matchDf.index)))
gameData = zip(matchDf['streakLength'], matchDf['streakType'], matchDf['playerWin'], matchDf['timeSinceLastGame'])
numGamesWon = len(list(matchDf[matchDf['playerWin']]))
numGamesLost = numGames - numGamesWon
longestStreak = int((matchDf['streakLength'].max()))

streaksVsStartNew = [0.0] * (longestStreak + 1)
winstreakVsWinrate = [0.0] * (longestStreak + 1)
loseStreakVsWinrate = [0.0] * (longestStreak + 1)
numGamesPerWinstreak = [0.0] * (longestStreak + 1)
numGamesPerLosestreak = [0.0] * (longestStreak + 1)
# Bit of an if/else ladder, not pretty but eh its fine
for g in gameData:
    # The game has a not-nan streak
    if g[0] == g[0]:
        streakIndex = int(g[0])
        # if on a winstreak
        if 'win' in g[1]:
            # add to game counter
            numGamesPerWinstreak[streakIndex] += 1
            # if game was won, add to win counter
            if g[2]:
                winstreakVsWinrate[streakIndex] += 1
        else:
            numGamesPerLosestreak[streakIndex] += 1
            if g[2]:
                loseStreakVsWinrate[streakIndex] += 1
        
        # if new game started shortly after
        if g[3] < streakTime:
            # add to respective streak counter
            streaksVsStartNew[streakIndex] += 1

ssnX = list(range(0, longestStreak + 1))
ssnY = streaksVsStartNew
ssnY = [x/(gcount1 + gcount2) if (gcount1+gcount2 > 0) else 0 for x, gcount1, gcount2 in zip(ssnY, numGamesPerWinstreak, numGamesPerLosestreak)]

wstreakWRX = list(range(0, longestStreak + 1))
wstreakWRY = winstreakVsWinrate
wstreakWRY = [x/wincount if wincount>0 else 0 for x, wincount in zip(wstreakWRY, numGamesPerWinstreak)]

lstreaWRX = list(range(0, longestStreak + 1))
lstreaWRY = loseStreakVsWinrate
lstreaWRY = [x/losecount if losecount>0 else 0 for x, losecount in zip(lstreaWRY, numGamesPerLosestreak)]

# Least squares line fit is kinda garbage for this sample size actually
# a single 1.0 sample at streak 8 distorts this so much
'''ssnCorrelationX = repeatListByList(ssnX, streaksVsStartNew)
ssnCorrelationY = repeatListByList(ssnY, streaksVsStartNew)
wstreakWRCorrelationX = repeatListByList(wstreakWRX, winstreakVsWinrate)
wstreakWRCorrelationY = repeatListByList(wstreakWRY, winstreakVsWinrate)
lstreaWRCorrelationX = repeatListByList(lstreaWRX, loseStreakVsWinrate)
lstreaWRCorrelationY = repeatListByList(lstreaWRY, loseStreakVsWinrate)

repeatQueueCorrelation, repeatQueuePvalue = spearmanr(ssnCorrelationX, b=ssnCorrelationY, axis=0)
winnersQCorrelation, winnersQPvalue = spearmanr(wstreakWRCorrelationX, b=wstreakWRCorrelationY, axis=0)
losersQCorrelation, losersQPvalue = spearmanr(lstreaWRCorrelationX, b=lstreaWRCorrelationY, axis=0)
print('Correlation and P value for these queues being good player engagement: {}, {}'.format(repeatQueueCorrelation, repeatQueuePvalue))
print('Correlation and P value for winners queue being a thing: {}, {}'.format(winnersQCorrelation, winnersQPvalue))
print('Correlation and P value for losers queue being a thing: {}, {}'.format(losersQCorrelation, losersQPvalue))
ssnLineCoeffs =  polyfit(ssnCorrelationX, ssnCorrelationY, 1)
wstreakWRLineCoeffs =  polyfit(wstreakWRCorrelationX, wstreakWRCorrelationY, 1)
lstreakWRLineCoeffs =  polyfit(lstreaWRCorrelationX, lstreaWRCorrelationY, 1)
ssnLineY = [ssnLineCoeffs[1] + ssnLineCoeffs[0]*x for x in ssnX]
wstreakWRLineY = [wstreakWRLineCoeffs[1] + wstreakWRLineCoeffs[0]*x for x in wstreakWRX]
lstreakWRLineY = [lstreakWRLineCoeffs[1] + lstreakWRLineCoeffs[0]*x for x in lstreaWRX]'''

fig, ax = plt.subplots()
plt.ylim(bottom=0)
plt.xlim(left=0)
plt.xticks(list(range(1, longestStreak+1)))
winstreakDots = plt.scatter(ssnX[1:], ssnY[1:], 10, marker='o', c='#5555ffff')
#winstreakLine = plt.plot(ssnX[1:], ssnLineY[1:])
plt.title("Winstreak size vs chance of {} queueing up for another ranked game".format(personName))

plt.draw()
plt.savefig('{}_queues_up_again.png'.format(personName))

plt.figure()
winstreakDots = plt.scatter(wstreakWRX[1:], wstreakWRY[1:], 10, marker='o', c='#88aa88ff')
#winstreakLine = plt.plot(wstreakWRX[1:], wstreakWRLineY[1:])
plt.title("Winstreak size vs {} winrate".format(personName))
plt.draw()
plt.savefig('{}_winrate_vs_winstreak_size.png'.format(personName))

plt.figure()
winstreakDots = plt.scatter(lstreaWRX[1:], lstreaWRY[1:], 10, marker='o', c='#ff5555ff')
#winstreakLine = plt.plot(lstreaWRX[1:], lstreakWRLineY[1:])
plt.title("Losestreak size vs {} winrate".format(personName))
plt.draw()
plt.savefig('{}_winrate_vs_losestreak_size.png'.format(personName))