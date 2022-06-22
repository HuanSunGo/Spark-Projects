import matplotlib.pyplot as plt 

# bring in the pyplot interface
fig, ax = plt.subplots( )  
plt.show( ) 

# put two plots together 
fig, ax = plt.subplots()
ax.plot(seattle_weather["MONTH"], seattle_weather["MLY-TAVG-NORMAL"])
ax.plot(austin_weather["MONTH"], austin_weather["MLY-TAVG-NORMAL"])
plt.show( )

# customize by adding details  
ax.plot(seattle_weather["MONTH"],
    seattle_weather["MLY-PRCP-NORMAL"],
    color="b", ## specifying color 
    linestyle="--", ## specifying dotted line
    marker="o") ## adding markers: "v" are arrows 

# customize the axes labels
ax.set_xlabel("Time(month)")
ax.set_ylabel("Whatever")

# add title 
ax.set_title("Fancy Name")