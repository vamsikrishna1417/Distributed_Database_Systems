
The task is two write two functions described below, which will perform some textual and spatial searching on MongoDB.

a. FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection)
This function searches the ‘collection’ given to find all the business present
in the city provided in ‘cityToSearch’ and save it to ‘saveLocation1’.

b. FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection)
This function searches the ‘collection’ given to find name of all the business
present in the ‘maxDistance’ from the given ‘myLocation’ that covers all the
given categories (please use the distance algorithm given below) and save them
to ‘saveLocation2’. Each line of the output file will contain the name of the
business only.
