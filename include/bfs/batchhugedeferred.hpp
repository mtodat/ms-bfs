//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
%%=========================
%% Assistenten(PersNr, Name, Fachgebiet, Boss)
assistenten(3002,platon,ideenlehre,2125).
assistenten(3003,aristoteles,syllogistik,2125).
assistenten(3004,wittgenstein,sprachtheorie,2126).
assistenten(3005,rhetikus,planetenbewegung,2127).
assistenten(3006,newton,keplersche_gesetze,2127).
assistenten(3007,spinoza,gott_und_natur,2134).

%% hoeren(MatrNr, VorlNr)
hoeren(26120,5001).
hoeren(27550,5001).
hoeren(27550,4052).
hoeren(28106,5041).
hoeren(28106,5052).
hoeren(28106,5216).
hoeren(28106,5259).
hoeren(29120,5001).
hoeren(29120,5041).
hoeren(29120,5049).
hoeren(29555,5022).
hoeren(25403,5022).
hoeren(29555,5001).

%% pruefen(MatrNr,VorlNr, PersNr, Note)
pruefen(28106,5001,2126,1).
pruefen(25403,5041,2125,2).
pruefen(27550,4630,2137,2).

%%Vorlesungen(VorlNr, Titel, SWS, gelesenVon)
vorlesungen(5001,grundzuege,4,2137).
vorlesungen(5041,ethik,4,2125).
vorlesungen(5043,erkenntnistheorie,3,2126).
vorlesungen(5049,maeeutik,2,2125).
vorlesungen(4052,logik,4,2125).
vorlesungen(5052,wissenschaftstheorie,3,2126).
vorlesungen(5216,bioethik,2,2126).
vorlesungen(5259,der_wiener_kreis,2,2133).
vorlesungen(5022,glaube_und_wissen,2,2134).
vorlesungen(4630,die_3_kritiken,4,2137).

%%Professoren(PersNr,Name,Rang, Raum)
professoren(2125,sokrates,c4,226).
professoren(2126,russel,c4,232).
professoren(2127,kopernikus,c3,310).
professoren(2133,popper,c3,52).
professoren(2134,augustinus,c3,309).
professoren(2136,curie,c4,36).
professoren(2137,kant,c4,7).

%%voraussetzen(Vorg,Nachf)
voraussetzen(5001,5041).
voraussetzen(5001,5043).
voraussetzen(5001,5049).
voraussetzen(5041,5216).
voraussetzen(5043,5052).
voraussetzen(5041,5052).
voraussetzen(5052,5259).

%%Studenten(MatrNr, Name, Semester)
studenten(24002,xenokrates,18).
studenten(25403,jonas,12).
studenten(26120,fichte,10).
studenten(26830,aristoxenos,8).
studenten(27550,schopenhauer,6).
studenten(28106,carnap,3).
studenten(29120,theophrastos,2).
studenten(29555,feuerbach,2).