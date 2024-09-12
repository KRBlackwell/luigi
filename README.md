# luigi
##luigi sas example: programs run in sequence
As an example of how an already existing automated workflow like luigi could help the survey. It's currently orchestrated by custom scripts, but using something that's already built out with all the bells and whistles (some we need to include going forward) would give us the flexibility to add in some parallel processing going forward. However, that would require separating the data in a new way. Since it's processed in sequence, it doesn't have to be separated as much, just enough for SAS to be able to handle them, and then they're separated into the level of data. There are also dependent files delivered from outside vendors, other systems in other divisions, and cleaned by outside processes.
The resources and locations listed in the program are not the same names as what we really use.
[Python program](https://github.com/KRBlackwell/luigi/blob/main/luigi_sas_example.py)\
[program listing](https://github.com/KRBlackwell/luigi/blob/main/luigi_sas_example.json)\
