import os
from datetime import datetime
import getpass

def lasting(pdato, luser, tab, filsti=None, fil=None, dbase="TEST",
                     pord=None, overskriv=1, godkjenn=2, mailto=None):
    """
    Forklaring:
    Funksjon for å laste opp til statistikkbanken vha. Python,
    kan laste til en tabell og en fil med tilhørende filsti.
    -
    Inputs:
    pdato = Publiseringsdato YYYYMMDD
    luser = Lastebruker for seksjonen.
    tab = Tabell i statistikkbanken. Dette kan ta i mot
          string, liste, eller en dictionary avhengig av metode
          en vil utføre. String hvis det skal lastes opp til en
          tabell med en (ev. flere) fil, en liste hvis det skal
          lastes opp til en tabell med flere filer, en
          dictionary hvis en vil laste opp til flere tabeller med
          en eller flere tilhørende filer. Se nærmere beskrivelse
          i eget avsnitt under.
    filsti = Filsti hvor filen ligger hen hvis en eller flere filer
             skal lastes opp.
    fil = Navne på filen som skal lastes opp hvis en eller flere
          filer skal lastes opp.
    dbase = Server lastingen foregår på; TEST for testing,
            og PROD for produksjon, ev. QA.
    pord = Passord til lastebrukeren. Er valgfritt. Hvis ingen
           input her så blir du bedt om å taste inn passordet i
           spørringen.
    overskriv = Autooverskriv: 1 for ja og 0 for nei
    godkjenn: 0 = Manuell godkjenning
              1 = Automatisk ved lasting (umiddelbart)
              2 = JIT (Just-in-time, dvs. rett før publisering)
    mailto = Brukernavn til den som skal motta mail. Er valgfritt.
             Hvis ingen input vil brukeren av funksjonen få mail.
    -
    Nærmere beskrivelse:
    For å laste opp flere filer til en tabell kan man enten sett opp
    alle filer i en string hvor mellomrom skiller dem fra hverandre,
    eller sette dem i en liste.
    For å laste opp til flere tabeller med en eller flere filer så må
    det settes opp en nested dictionary på forhånd.
    Eksempel: data = {"tabell": {"filsti": ["Filnavn1.dat", "Filnavn2.dat"]}}
    De aller fleste inputs til funksjonen må være i string format,
    mens overskriv og godkjent helst som integers.
    """
    # Kontroll sjekk
    try:
        datetime.strptime(pdato, "%Y%m%d")
    except ValueError:
        raise ValueError('Publiseringsdato har enten feil format eller skrevet feil! Må være string og YYYYMMDD')
    if type(luser) is not str:
        raise ValueError('Input user for lastebruker er ikke en string')
    if type(dbase) is not str:
        raise ValueError('Input dbase er ikke en string')
    if type(overskriv) is not int:
        raise ValueError('Input overskriv er ikke en integer')
    if type(godkjenn) is not int:
        raise ValueError('Input godkjenn er ikke en integer')
    if mailto is None:
        init = getpass.getuser()
    else:
        init = mailto
        if type(init) is not str:
            raise ValueError('Input mailto for mailmottaker er ikke en string')
    if pord is None:
        passw = getpass.getpass(f"Passord til lastebruker {luser} for {dbase}:")
    else:
        passw = pord
        if type(passw) is not str:
            raise ValueError('Input passord for lastebruker er ikke en string')
    publdato = datetime.strftime(datetime.strptime(pdato, "%Y%m%d"), "%Y.%m.%d")
    # Diverse tekst strings for shell kommandoer
    perl_path = f"/ssb/stamme04/statbas/system/prog/filoverforing/{dbase}/overforing.pl"
    laste_path = f"/ssb/stamme04/statbas/system/prog/bestillLasting/{dbase}/bestillLasting.sh"
    overforlog = f"/tmp/{luser}_overforing_sas.log"
    bestillelog = f"/tmp/{luser}_bestillLasting_sas.log"
    # Setter miljøvariabel
    os.environ['STATBAS'] = "/ssb/stamme04/statbas"
    # Starter opplastingen
    if tab is not None and filsti is not None and fil is not None:
        if type(tab) is not str:
            raise ValueError('Input tab for tabell er ikke en string')
        if type(filsti) is not str:
            raise ValueError('Input filsti er ikke en string')
        if filsti[-1] != '/':
            filsti = filsti + '/'
        if type(fil) is list:
            stiogfiler = [f"{filsti}{i}" for i in fil]
            datfiles = ' '.join(stiogfiler)
        elif type(fil) is str and ' ' in fil:
            fillist = fil.split(' ')
            stiogfiler = [f"{filsti}{i}" for i in fillist]
            datfiles = ' '.join(stiogfiler)
        elif type(fil) is str:
            datfiles = filsti + fil
        else:
            raise ValueError('Input fil er ikke en string eller en liste av strings')
        print(f"Starter opplasting for {tab}")
        print(f"Fil(er) for opplasting: {datfiles}")
        # Setter sammen kommando for perl scriptet
        perl_script = f"{perl_path} -o /tmp -a {luser} {passw} {datfiles} > {overforlog} 2>&1"
        # Kjør Perl script
        os.system(perl_script)
        # Åpne logg for overføring og sjekk at det ser greit ut
        with open(overforlog, 'rb') as log:
            textlist = log.read().splitlines()
            log.close()
        print(textlist)
        os.remove(overforlog)
        text1 = textlist[-1].decode("utf-8")
        # Kjør shell script hvis ok
        if 'Programmet er ferdig' in text1:
            BestillLast = f"{laste_path} {luser} {passw} {tab} {publdato} 10:00 {init} {overskriv} {godkjenn} > {bestillelog} 2>&1"
            os.system(BestillLast)
            with open(bestillelog, 'rb') as last:
                laste_log = last.read().splitlines()
                last.close()
            print(laste_log)  # Ser det greit ut?
            os.remove(bestillelog)
    elif type(tab) is dict and filsti is None and fil is None:
        tabeller = list(tab.keys())
        for tabell in tabeller:
            if type(tabell) is not str:
                raise ValueError('Forventet en String fra dict, men har mottatt noe annet!')
            print(f"Starter opplasting for {tabell}")
            filplass = list(tab.get(tabell).keys())
            filene = []
            for fplass in filplass:
                if type(fplass) is not str:
                    raise ValueError('Forventet en String fra dict, men har mottatt noe annet!')
                datfiler = tab.get(tabell).get(fplass)
                if fplass[-1] != '/':
                    fplass = fplass + '/'
                if type(datfiler) == list:
                    for f in datfiler:
                        if type(f) is not str:
                            raise ValueError('Forventet en String fra dict, men har mottatt noe annet!')
                        sti = fplass + f
                        filene.append(sti)
                else:
                    sti = fplass + datfiler
                    filene.append(sti)
            if len(filene) == 1:
                datfiles = filene[0]
            else:
                datfiles = ' '.join(filene)
            print(f"Fil(er) for opplasting: {datfiles}")
            # Setter sammen kommando for perl scriptet
            perl_script = f"{perl_path} -o /tmp -a {luser} {passw} {datfiles} > {overforlog} 2>&1"
            # Kjør Perl script
            os.system(perl_script)
            # Åpne logg for overføring og sjekk at det ser greit ut
            with open(overforlog, 'rb') as log:
                textlist = log.read().splitlines()
                log.close()
            print(textlist)
            text1 = textlist[-1].decode("utf-8")
            # Kjør shell script hvis ok
            if 'Programmet er ferdig' in text1:
                BestillLast = f"{laste_path} {luser} {passw} {tabell} {publdato} 10:00 {init} {overskriv} {godkjenn} > {bestillelog} 2>&1"
                os.system(BestillLast)
                with open(bestillelog, 'rb') as last:
                    laste_log = last.read().splitlines()
                    last.close()
                print(laste_log) # Ser det greit ut?
            # Sletter logger
            os.remove(overforlog)
            os.remove(bestillelog)
    else:
        raise ValueError('Input kombinasjonene til denne funksjonen er merkelige, sjekk nærmere!')
    # Sletter passordet etter at alt er ferdig
    del passw
    print('Opplastning til Statistikkbanken er ferdig!')

    
if __name__ == '__main__':
    print("Only for importing?")