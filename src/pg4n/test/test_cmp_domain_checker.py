import pytest
from os import getenv
from pytest_postgresql import factories
import psycopg
from psycopg import Connection
import sqlglot
import sqlglot.expressions as exp

from ..sqlparser import SqlParser
from ..sqlparser import Column
from ..cmp_domain_checker import CmpDomainChecker

# These are useful so we can refer to the same table names in load_database and
# in the tests without horrible to debug bugs caused by a typo.
CUSTOMERS_TABLE_NAME = "e31_test_table_customers"
ORDERS_TABLE_NAME = "e31_test_table_orders"


def load_database(**kwargs):
    conn: Connection = psycopg.connect(**kwargs)

    with conn.cursor() as cur:
        cur.execute(
            f"""
        DROP TABLE IF EXISTS {ORDERS_TABLE_NAME};
        DROP TABLE IF EXISTS {CUSTOMERS_TABLE_NAME};

        CREATE TABLE {CUSTOMERS_TABLE_NAME} (
        customer_id INT
        , fname VARCHAR(50) NOT NULL
        , sname VARCHAR(50) NOT NULL
        , type CHAR(1) NOT NULL
            CHECK (type IN ('C', 'B')) -- C = customer, B = business
        , nickname VARCHAR(20) NOT NULL
        , PRIMARY KEY (customer_id)
        );

        CREATE TABLE {ORDERS_TABLE_NAME} (
            order_id        INT
        , order_total_eur DECIMAL(6,2) NOT NULL
        , customer_id     INT           NOT NULL
        , PRIMARY KEY (order_id)
        , FOREIGN KEY (customer_id)
            REFERENCES {CUSTOMERS_TABLE_NAME} (customer_id)
        );

        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (1, 'Josi', 'Grimsell', 'B', 'Aaron');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (2, 'Tomlin', 'Nozzolinii', 'B', 'Abbigail');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (3, 'Christen', 'Culley', 'C', 'Abednego');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (4, 'Nancey', 'Fawlkes', 'C', 'Abel');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (5, 'Callida', 'Tomasello', 'B', 'Abiel');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (6, 'Daisey', 'Hamill', 'B', 'Abigail');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (7, 'Lorin', 'Dollimore', 'C', 'Abijah');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (8, 'Gasparo', 'Bohlje', 'C', 'Abner');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (9, 'Lester', 'Markus', 'C', 'Abraham');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (10, 'Sonnie', 'Kelling', 'B', 'Abram');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (11, 'Amby', 'Ligoe', 'C', 'Absalom');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (12, 'Tiffi', 'Riolfo', 'C', 'Ada');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (13, 'Marion', 'Penelli', 'B', 'Adaline');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (14, 'Davon', 'Burris', 'C', 'Addison');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (15, 'Zachary', 'Faloon', 'C', 'Adela');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (16, 'Augustin', 'Blaxall', 'C', 'Adelaide');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (17, 'Carlyle', 'Zimek', 'B', 'Adelbert');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (18, 'Odie', 'Rowling', 'C', 'Adele');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (19, 'Daphne', 'Bullen', 'B', 'Adeline');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (20, 'Robbi', 'O''Caherny', 'B', 'Adelphia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (21, 'Nady', 'Lempertz', 'B', 'Adolphus');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (22, 'Grazia', 'Syne', 'C', 'Adrian');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (23, 'Jeanette', 'Fincher', 'B', 'Adriane');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (24, 'Jacinthe', 'Kleeman', 'B', 'Adrienne');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (25, 'Neely', 'Merrydew', 'B', 'Agatha');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (26, 'Jenica', 'Martina', 'C', 'Agnes');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (27, 'Callean', 'Werlock', 'C', 'Aileen');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (28, 'Nanice', 'MacMaster', 'B', 'Alan');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (29, 'Dawna', 'Knipe', 'C', 'Alanson');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (30, 'Colin', 'Jansie', 'C', 'Alastair');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (31, 'Dario', 'Siehard', 'C', 'Alazama');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (32, 'Aubrie', 'Lockitt', 'C', 'Albert');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (33, 'Amble', 'Jewes', 'B', 'Alberta');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (34, 'Lorens', 'Buzin', 'C', 'Aldo');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (35, 'Lainey', 'Davidow', 'C', 'Aldrich');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (36, 'Bendite', 'Morfett', 'B', 'Aleksandr');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (37, 'Talbot', 'Keddey', 'B', 'Aleva');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (38, 'Web', 'Catterill', 'B', 'Alex');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (39, 'Tabbie', 'Glison', 'B', 'Alexander');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (40, 'Olva', 'Leborgne', 'C', 'Alexandra');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (41, 'Reilly', 'Kennler', 'B', 'Alexandria');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (42, 'Pyotr', 'Feldhorn', 'C', 'Alexis');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (43, 'Belle', 'Barsham', 'B', 'Alfonse');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (44, 'Buffy', 'O''Cridigan', 'B', 'Alfred');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (45, 'Seymour', 'Mayer', 'B', 'Alfreda');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (46, 'Mechelle', 'Vinick', 'B', 'Algernon');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (47, 'Tracey', 'Sauvan', 'B', 'Alice');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (48, 'Marve', 'Eykel', 'C', 'Alicia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (49, 'Theodosia', 'Rosson', 'B', 'Aline');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (50, 'Anne-marie', 'Mounsie', 'B', 'Alison');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (51, 'Kennan', 'Burstowe', 'B', 'Alixandra');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (52, 'Gerrilee', 'Ackland', 'B', 'Allan');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (53, 'Riva', 'Scawen', 'C', 'Allen');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (54, 'Malchy', 'Hearty', 'B', 'Allisandra');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (55, 'Terrill', 'Syde', 'B', 'Allison');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (56, 'Kaitlin', 'Payle', 'B', 'Allyson');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (57, 'Vita', 'Dunnett', 'B', 'Allyssa');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (58, 'Lettie', 'Coffin', 'B', 'Almena');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (59, 'Lin', 'Race', 'B', 'Almina');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (60, 'Sibyl', 'Thoms', 'C', 'Almira');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (61, 'Blisse', 'Dillway', 'C', 'Alonzo');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (62, 'Seline', 'McGray', 'B', 'Alphinias');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (63, 'Gratia', 'Moss', 'B', 'Althea');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (64, 'Dyan', 'Crosbie', 'B', 'Alverta');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (65, 'Glenna', 'Alastair', 'C', 'Alyssa');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (66, 'Sadella', 'Boxhall', 'B', 'Alzada');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (67, 'Zsazsa', 'Bellocht', 'C', 'Amanda');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (68, 'Leisha', 'Darlington', 'C', 'Ambrose');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (69, 'Maryjo', 'Pink', 'C', 'Amelia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (70, 'Carmela', 'Sedgeworth', 'C', 'Amos');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (71, 'Cynthie', 'Rouby', 'B', 'Anastasia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (72, 'Nadiya', 'Gingles', 'B', 'Anderson');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (73, 'Seymour', 'Maffioletti', 'B', 'Andre');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (74, 'Giffy', 'Cottee', 'C', 'Andrea');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (75, 'Erma', 'Cranstone', 'B', 'Andrew');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (76, 'Ardys', 'Currm', 'B', 'Andriane');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (77, 'Maegan', 'Wheldon', 'C', 'Angela');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (78, 'Matilde', 'Froome', 'B', 'Angelica');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (79, 'Preston', 'Groves', 'B', 'Angelina');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (80, 'Petey', 'Colloby', 'B', 'Ann');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (81, 'Sharline', 'Rosenwald', 'B', 'Anna');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (82, 'Abey', 'Darridon', 'C', 'Anne');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (83, 'Wolf', 'Brenton', 'B', 'Annette');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (84, 'Birk', 'Malling', 'C', 'Annie');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (85, 'Brig', 'Triswell', 'C', 'Anselm');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (86, 'Lauren', 'Tolworth', 'B', 'Anthony');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (87, 'Nanette', 'McElwee', 'B', 'Antoinette');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (88, 'Sansone', 'Copsey', 'C', 'Antonia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (89, 'Nickie', 'Bloss', 'C', 'Antonio');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (90, 'Forest', 'Trim', 'B', 'Appoline');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (91, 'Bobby', 'Fortesquieu', 'B', 'Aquilla');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (92, 'Raven', 'Eilles', 'C', 'Ara');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (93, 'Sashenka', 'Fedorski', 'C', 'Arabella');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (94, 'Halimeda', 'Freak', 'B', 'Arabelle');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (95, 'Amandie', 'Botham', 'B', 'Araminta');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (96, 'Tiertza', 'Bunford', 'C', 'Archibald');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (97, 'Germain', 'Haly', 'B', 'Archilles');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (98, 'Elfreda', 'Tome', 'C', 'Ariadne');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (99, 'Phineas', 'Yuryaev', 'B', 'Arielle');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (100, 'Gypsy', 'Bottrell', 'C', 'Aristotle');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (101, 'Hamilton', 'Pellew', 'B', 'Arizona');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (102, 'Ricca', 'Rupprecht', 'B', 'Arlene');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (103, 'Ede', 'Golden of Ireland', 'C', 'Armanda');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (104, 'Timofei', 'Grene', 'B', 'Armena');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (105, 'Cary', 'Sells', 'C', 'Armilda');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (106, 'Merrily', 'Coundley', 'C', 'Arminda');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (107, 'Celisse', 'Rubra', 'B', 'Arminta');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (108, 'Janene', 'Motten', 'C', 'Arnold');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (109, 'Marquita', 'Impy', 'B', 'Aron');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (110, 'Donall', 'Labrenz', 'B', 'Artelepsa');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (111, 'Torey', 'Sessuns', 'B', 'Artemus');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (112, 'Nanci', 'Byrd', 'C', 'Arthur');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (113, 'Dania', 'Foxton', 'C', 'Arthusa');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (114, 'Fara', 'Arkil', 'B', 'Arzada');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (115, 'Shirl', 'Pilpovic', 'C', 'Asahel');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (116, 'Annmarie', 'Errichelli', 'B', 'Asaph');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (117, 'Marysa', 'Wadman', 'C', 'Asenath');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (118, 'Hermy', 'Whieldon', 'B', 'Ashley');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (119, 'Gregg', 'Lazenbury', 'B', 'Aubrey');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (120, 'Corinna', 'Pyburn', 'B', 'Audrey');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (121, 'Clywd', 'Stokey', 'B', 'August');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (122, 'Myrlene', 'Worcs', 'B', 'Augusta');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (123, 'Lyon', 'Nolleau', 'C', 'Augustina');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (124, 'Penelope', 'Quadling', 'C', 'Augustine');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (125, 'Leo', 'Creaney', 'B', 'Augustus');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (126, 'Lilllie', 'Annis', 'C', 'Aurelia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (127, 'Rhoda', 'Kurton', 'C', 'Avarilla');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (128, 'Jere', 'Cometti', 'B', 'Azariah');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (129, 'Ashla', 'De-Ville', 'C', 'Bab');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (130, 'Rora', 'Adcock', 'C', 'Babs');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (131, 'Ive', 'Matches', 'B', 'Barbara');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (132, 'Olympe', 'Faber', 'B', 'Barbery');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (133, 'Clementia', 'Fergusson', 'B', 'Barbie');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (134, 'Georgetta', 'Crossley', 'B', 'Barnabas');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (135, 'Janenna', 'McCamish', 'C', 'Barney');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (136, 'Aretha', 'Arias', 'C', 'Bart');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (137, 'Lucilia', 'Bentick', 'C', 'Bartholomew');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (138, 'Ainsley', 'Wraighte', 'C', 'Barticus');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (139, 'Kelsi', 'Suddell', 'C', 'Bazaleel');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (140, 'Suki', 'Flinn', 'B', 'Bea');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (141, 'Jacinta', 'Villiers', 'B', 'Beatrice');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (142, 'Elfrieda', 'Cleary', 'C', 'Becca');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (143, 'Sasha', 'Eunson', 'C', 'Beck');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (144, 'Ange', 'Pasterfield', 'C', 'Bedelia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (145, 'Adena', 'Wenham', 'C', 'Belinda');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (146, 'Dunstan', 'Cressey', 'B', 'Bella');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (147, 'Wilmer', 'MacWhan', 'B', 'Benedict');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (148, 'Lidia', 'Quinane', 'C', 'Benjamin');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (149, 'Erik', 'Cushe', 'B', 'Benjy');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (150, 'Agnesse', 'Liebmann', 'B', 'Bernard');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (151, 'Prentiss', 'Filby', 'B', 'Berney');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (152, 'Towny', 'Middell', 'B', 'Bert');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (153, 'Alfy', 'McCahill', 'B', 'Bertha');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (154, 'Conney', 'Riteley', 'C', 'Bertram');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (155, 'Murdock', 'Dix', 'C', 'Bess');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (156, 'Allistir', 'Frary', 'C', 'Beth');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (157, 'Noach', 'MacRitchie', 'C', 'Bethena');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (158, 'Anett', 'Hercules', 'B', 'Beverly');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (159, 'Carilyn', 'Vinecombe', 'B', 'Bezaleel');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (160, 'Patti', 'Louis', 'C', 'Biddie');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (161, 'Barron', 'Dishmon', 'B', 'Bill');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (162, 'Jessee', 'List', 'B', 'Billy');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (163, 'Gerhardt', 'Simcock', 'B', 'Blanche');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (164, 'Abel', 'Bezants', 'C', 'Bob');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (165, 'Angele', 'Wildbore', 'C', 'Bobby');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (166, 'Angelo', 'Vanstone', 'C', 'Boetius');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (167, 'Ninnette', 'Steere', 'C', 'Brad');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (168, 'Melicent', 'Cheston', 'B', 'Bradford');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (169, 'Elsbeth', 'Nucciotti', 'B', 'Bradley');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (170, 'Bard', 'Shaw', 'C', 'Brady');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (171, 'Fredrick', 'Matuszynski', 'C', 'Breanna');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (172, 'Pammi', 'Codling', 'B', 'Breeanna');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (173, 'Kristopher', 'Mackelworth', 'B', 'Brenda');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (174, 'Carolyn', 'Quinlan', 'C', 'Brian');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (175, 'Davy', 'Blomefield', 'B', 'Brianna');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (176, 'Marylou', 'Guilloton', 'C', 'Bridget');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (177, 'Genvieve', 'Marthen', 'B', 'Brittany');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (178, 'Rogers', 'Plose', 'B', 'Brittney');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (179, 'Daphne', 'Kells', 'C', 'Broderick');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (180, 'Leonelle', 'Chiommienti', 'B', 'Caitlin');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (181, 'Daria', 'Wistance', 'C', 'Caitlyn');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (182, 'Sasha', 'Stiffkins', 'C', 'Caldonia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (183, 'Harvey', 'Skinn', 'B', 'Caleb');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (184, 'Louise', 'Jansey', 'C', 'California');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (185, 'Tanitansy', 'Headington', 'C', 'Calista');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (186, 'Chloette', 'Ratt', 'B', 'Calpurnia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (187, 'Donall', 'Casolla', 'B', 'Calvin');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (188, 'Ferguson', 'Mackriell', 'B', 'Cameron');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (189, 'Merle', 'Erridge', 'C', 'Camille');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (190, 'Kara', 'Backs', 'B', 'Campbell');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (191, 'Frankie', 'Canny', 'B', 'Candace');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (192, 'Dudley', 'Vesco', 'B', 'Carlotta');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (193, 'Brucie', 'Coning', 'C', 'Carlton');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (194, 'Corilla', 'Aldwinckle', 'C', 'Carmellia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (195, 'Dewey', 'Bottom', 'C', 'Carmelo');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (196, 'Louisa', 'Matasov', 'B', 'Carmon');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (197, 'Missie', 'Fealty', 'C', 'Carol');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (198, 'Kellby', 'Threlfall', 'C', 'Carolann');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (199, 'Matilda', 'Malenoir', 'C', 'Caroline');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (200, 'Fanni', 'Iacopetti', 'C', 'Carolyn');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (201, 'Arv', 'Spawforth', 'B', 'Carrie');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (202, 'Sonja', 'Pentycross', 'B', 'Carthaette');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (203, 'Rory', 'Hallatt', 'C', 'Casey');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (204, 'Cassy', 'Pearcehouse', 'C', 'Casper');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (205, 'Orlan', 'Kynder', 'C', 'Cassandra');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (206, 'Ardyth', 'Conningham', 'B', 'Cassidy');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (207, 'Elise', 'Kamenar', 'C', 'Caswell');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (208, 'Legra', 'Drought', 'C', 'Catherine');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (209, 'Dulcea', 'Akister', 'C', 'Cathleen');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (210, 'Pamelina', 'Vittery', 'C', 'Cathy');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (211, 'Jobi', 'Bails', 'B', 'Cecilia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (212, 'Annecorinne', 'Soles', 'B', 'Cedric');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (213, 'Rubin', 'Lunney', 'B', 'Celeste');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (214, 'Chalmers', 'Britton', 'C', 'Celinda');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (215, 'Nollie', 'Jemmett', 'B', 'Charity');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (216, 'Elsbeth', 'MacGaughie', 'B', 'Charles');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (217, 'Lucy', 'Durtnell', 'C', 'Charlie');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (218, 'Raffarty', 'Sweeney', 'C', 'Charlotte');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (219, 'Adriane', 'Ccomini', 'B', 'Chauncey');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (220, 'Kylen', 'Bewicke', 'B', 'Cheryl');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (221, 'Cassondra', 'Mattingson', 'B', 'Chesley');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (222, 'Hazel', 'Rushforth', 'C', 'Chester');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (223, 'Annice', 'Haynes', 'B', 'Chet');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (224, 'Dayle', 'Eirwin', 'B', 'Chick');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (225, 'Egbert', 'Vasyunin', 'B', 'Chloe');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (226, 'Emelina', 'Ayliff', 'B', 'Chris');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (227, 'Jeralee', 'Atwill', 'C', 'Christa');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (228, 'Rudd', 'Ames', 'B', 'Christian');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (229, 'Melessa', 'MacAleese', 'C', 'Christiana');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (230, 'Cordell', 'Bineham', 'B', 'Christiano');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (231, 'Calhoun', 'BoHlingolsen', 'C', 'Christina');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (232, 'Maritsa', 'Cowpland', 'C', 'Christine');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (233, 'Tibold', 'Sleigh', 'C', 'Christoph');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (234, 'Cayla', 'Statter', 'B', 'Christopher');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (235, 'Justus', 'Barthrup', 'B', 'Christy');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (236, 'Garnette', 'Tabrett', 'C', 'Cicely');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (237, 'Jeramie', 'Gallehock', 'C', 'Cinderella');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (238, 'Gregor', 'Crinion', 'B', 'Cindy');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (239, 'Atalanta', 'Girdler', 'C', 'Claire');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (240, 'Britte', 'Muge', 'B', 'Clara');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (241, 'Sigismund', 'Mowsdill', 'B', 'Clare');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (242, 'Brenden', 'Simmons', 'B', 'Clarence');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (243, 'Hannah', 'Mattheis', 'C', 'Clarinda');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (244, 'Katrine', 'Janiak', 'B', 'Clarissa');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (245, 'Holly', 'Roy', 'C', 'Claudia');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (246, 'Albert', 'Heimann', 'B', 'Cleatus');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (247, 'Dulcie', 'Crutchley', 'B', 'Clement');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (248, 'Norby', 'Biernacki', 'C', 'Clementine');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (249, 'Rhoda', 'Leheude', 'C', 'Cliff');
        insert into {CUSTOMERS_TABLE_NAME} (customer_id, fname, sname, type, nickname) values (250, 'Langston', 'Prosser', 'C', 'Clifford');

        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (1, 535.36, 111);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (2, 409.8, 217);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (3, 189.43, 19);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (4, 144.14, 157);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (5, 582.52, 172);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (6, 132.85, 206);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (7, 183.92, 236);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (8, 424.8, 244);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (9, 519.43, 175);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (10, 414.55, 234);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (11, 88.19, 50);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (12, 591.72, 143);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (13, 503.52, 216);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (14, 586.06, 181);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (15, 47.79, 248);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (16, 330.92, 130);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (17, 302.31, 225);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (18, 438.38, 26);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (19, 107.53, 94);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (20, 207.6, 9);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (21, 471.12, 179);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (22, 193.12, 6);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (23, 236.48, 51);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (24, 538.88, 38);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (25, 83.54, 79);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (26, 590.52, 50);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (27, 137.86, 21);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (28, 87.44, 1);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (29, 217.18, 124);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (30, 435.57, 105);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (31, 394.48, 62);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (32, 331.93, 144);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (33, 140.92, 236);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (34, 64.76, 13);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (35, 389.81, 218);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (36, 225.6, 136);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (37, 322.11, 41);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (38, 366.31, 59);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (39, 42.62, 95);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (40, 33.89, 23);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (41, 158.78, 145);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (42, 561.85, 222);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (43, 133.73, 196);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (44, 537.56, 2);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (45, 46.05, 177);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (46, 362.72, 240);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (47, 163.79, 141);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (48, 199.52, 234);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (49, 376.9, 89);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (50, 306.71, 235);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (51, 244.27, 119);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (52, 81.47, 212);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (53, 592.15, 207);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (54, 591.44, 145);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (55, 391.93, 128);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (56, 501.06, 129);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (57, 349.14, 58);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (58, 10.24, 74);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (59, 141.8, 118);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (60, 473.05, 208);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (61, 343.91, 223);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (62, 418.98, 60);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (63, 542.48, 52);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (64, 534.3, 108);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (65, 318.64, 6);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (66, 454.41, 11);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (67, 550.21, 63);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (68, 73.27, 189);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (69, 266.3, 39);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (70, 596.2, 221);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (71, 175.29, 115);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (72, 539.87, 62);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (73, 144.33, 52);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (74, 63.02, 117);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (75, 238.65, 203);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (76, 281.92, 183);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (77, 335.23, 149);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (78, 569.4, 99);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (79, 94.87, 194);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (80, 17.1, 21);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (81, 490.65, 41);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (82, 106.39, 237);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (83, 451.45, 203);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (84, 396.02, 111);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (85, 404.96, 79);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (86, 537.64, 201);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (87, 441.16, 191);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (88, 210.95, 158);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (89, 556.68, 74);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (90, 169.25, 30);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (91, 331.76, 115);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (92, 156.72, 192);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (93, 452.63, 115);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (94, 406.59, 129);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (95, 367.31, 48);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (96, 311.78, 173);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (97, 313.39, 241);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (98, 177.22, 86);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (99, 354.37, 66);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (100, 424.83, 50);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (101, 328.66, 189);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (102, 158.53, 220);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (103, 14.83, 183);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (104, 97.04, 11);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (105, 107.29, 12);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (106, 236.57, 4);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (107, 23.63, 179);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (108, 294.25, 131);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (109, 170.82, 206);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (110, 144.28, 59);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (111, 490.85, 191);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (112, 38.97, 239);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (113, 305.06, 123);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (114, 339.9, 108);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (115, 351.68, 153);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (116, 292.86, 97);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (117, 23.13, 46);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (118, 23.96, 195);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (119, 579.12, 43);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (120, 454.18, 214);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (121, 132.25, 50);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (122, 406.1, 80);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (123, 370.87, 115);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (124, 552.02, 191);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (125, 170.66, 131);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (126, 384.61, 226);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (127, 451.77, 17);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (128, 415.86, 243);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (129, 426.3, 136);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (130, 575.58, 235);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (131, 255.66, 127);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (132, 488.95, 234);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (133, 499.73, 235);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (134, 296.72, 222);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (135, 574.1, 222);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (136, 411.87, 10);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (137, 417.16, 215);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (138, 223.81, 183);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (139, 441.16, 97);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (140, 297.41, 101);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (141, 183.27, 44);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (142, 509.25, 190);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (143, 239.98, 109);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (144, 504.73, 154);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (145, 112.51, 162);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (146, 184.13, 22);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (147, 180.39, 2);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (148, 203.36, 180);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (149, 340.62, 215);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (150, 439.75, 18);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (151, 221.16, 212);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (152, 291.54, 10);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (153, 350.78, 247);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (154, 291.52, 54);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (155, 274.14, 151);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (156, 130.63, 188);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (157, 477.53, 205);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (158, 65.82, 201);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (159, 281.36, 198);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (160, 144.6, 82);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (161, 184.39, 94);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (162, 168.92, 27);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (163, 61.42, 69);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (164, 193.39, 169);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (165, 84.61, 1);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (166, 95.01, 189);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (167, 547.07, 197);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (168, 137.02, 109);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (169, 552.88, 241);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (170, 539.11, 92);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (171, 175.72, 249);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (172, 15.3, 30);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (173, 297.4, 85);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (174, 597.34, 184);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (175, 161.61, 58);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (176, 22.8, 175);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (177, 334.0, 167);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (178, 563.08, 161);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (179, 453.95, 43);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (180, 452.67, 234);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (181, 150.37, 69);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (182, 478.47, 70);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (183, 546.08, 88);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (184, 380.46, 98);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (185, 505.7, 49);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (186, 561.85, 156);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (187, 271.52, 242);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (188, 495.68, 39);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (189, 164.91, 68);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (190, 401.19, 88);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (191, 183.55, 142);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (192, 206.39, 151);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (193, 449.91, 213);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (194, 328.76, 41);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (195, 504.28, 117);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (196, 259.0, 244);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (197, 529.63, 46);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (198, 120.47, 95);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (199, 376.5, 53);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (200, 592.16, 137);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (201, 411.07, 202);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (202, 99.52, 171);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (203, 545.33, 116);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (204, 212.04, 203);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (205, 508.59, 59);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (206, 226.62, 228);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (207, 205.25, 199);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (208, 344.91, 150);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (209, 203.86, 244);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (210, 332.45, 47);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (211, 399.2, 138);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (212, 179.55, 180);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (213, 329.78, 105);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (214, 520.52, 98);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (215, 515.79, 96);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (216, 388.15, 142);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (217, 302.24, 189);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (218, 389.51, 177);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (219, 594.84, 121);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (220, 367.15, 163);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (221, 487.39, 227);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (222, 416.65, 59);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (223, 200.44, 47);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (224, 293.35, 186);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (225, 26.43, 228);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (226, 212.04, 28);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (227, 189.25, 230);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (228, 542.3, 42);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (229, 89.34, 147);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (230, 389.39, 247);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (231, 51.77, 118);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (232, 330.85, 204);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (233, 87.21, 179);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (234, 254.56, 98);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (235, 230.72, 168);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (236, 485.18, 167);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (237, 293.23, 140);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (238, 448.86, 16);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (239, 327.06, 34);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (240, 384.87, 107);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (241, 296.03, 169);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (242, 356.69, 244);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (243, 209.34, 31);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (244, 273.35, 153);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (245, 327.9, 212);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (246, 510.74, 188);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (247, 123.55, 179);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (248, 321.97, 195);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (249, 491.05, 63);
        insert into {ORDERS_TABLE_NAME} (order_id, order_total_eur, customer_id) values (250, 367.56, 214);""")
        conn.commit()


factory = factories.postgresql_proc(
    load=[load_database],
)
postgresql = factories.postgresql("factory")


@pytest.fixture
def sql_parser(postgresql: Connection):
    return SqlParser(db_connection=postgresql)


def test_check(sql_parser: SqlParser):

    SQL_VARCHAR_SUSPICIOUS = \
        f"""
SELECT customer_id
FROM {CUSTOMERS_TABLE_NAME}
WHERE fname > nickname;"""

    SQL_MULTI_VARCHAR_SUSPICIOUS = \
        f"""
SELECT customer_id
FROM {CUSTOMERS_TABLE_NAME}
WHERE fname > nickname AND
      sname < nickname AND
      customer_id = customer_id;"""

    SQL_INT_VARCHAR_VALID = \
        f"""
SELECT customer_id
FROM {CUSTOMERS_TABLE_NAME}
WHERE fname > customer_id;"""

    SQL_MULTI_CMP_VALID = \
        f"""
SELECT *
FROM {ORDERS_TABLE_NAME}
WHERE (order_total_eur = order_total_eur) OR
      (order_total_eur = 0 AND order_total_eur = 100);"""

    SQL_NESTED_WHERE_VALID = \
        f"""
SELECT *
FROM {ORDERS_TABLE_NAME}
WHERE (order_total_eur = order_total_eur) AND
       customer_id IN (SELECT c.customer_id
                       FROM {CUSTOMERS_TABLE_NAME} AS c
                       WHERE c.type = 'B');"""

    SQL_NESTED_WHERE_SUSPICIOUS = \
        f"""
SELECT *
FROM {ORDERS_TABLE_NAME}
WHERE (order_total_eur = order_total_eur) AND
       customer_id IN (SELECT c.customer_id
                       FROM {CUSTOMERS_TABLE_NAME} AS c
                       WHERE c.fname > c.nickname);"""

    sql_statement = SQL_VARCHAR_SUSPICIOUS
    parsed_sql = sql_parser.parse_one(sql_statement)
    columns = sql_parser.get_query_columns(parsed_sql)
    checker = CmpDomainChecker(parsed_sql, columns)
    assert checker is not None
    warning_msg = checker.check()
    assert warning_msg is not None

    sql_statement = SQL_MULTI_VARCHAR_SUSPICIOUS
    parsed_sql = sql_parser.parse_one(sql_statement)
    columns = sql_parser.get_query_columns(parsed_sql)
    checker = CmpDomainChecker(parsed_sql, columns)
    assert checker is not None
    warning_msg = checker.check()
    assert warning_msg is not None

    sql_statement = SQL_INT_VARCHAR_VALID
    parsed_sql = sql_parser.parse_one(sql_statement)
    columns = sql_parser.get_query_columns(parsed_sql)
    checker = CmpDomainChecker(parsed_sql, columns)
    assert checker is not None
    warning_msg = checker.check()
    assert warning_msg is None

    sql_statement = SQL_MULTI_CMP_VALID
    parsed_sql = sql_parser.parse_one(sql_statement)
    columns = sql_parser.get_query_columns(parsed_sql)
    checker = CmpDomainChecker(parsed_sql, columns)
    assert checker is not None
    warning_msg = checker.check()
    assert warning_msg is None

    sql_statement = SQL_NESTED_WHERE_VALID
    parsed_sql = sql_parser.parse_one(sql_statement)
    columns = sql_parser.get_query_columns(parsed_sql)
    checker = CmpDomainChecker(parsed_sql, columns)
    assert checker is not None
    warning_msg = checker.check()
    assert warning_msg is None

    sql_statement = SQL_NESTED_WHERE_SUSPICIOUS
    parsed_sql = sql_parser.parse_one(sql_statement)
    columns = sql_parser.get_query_columns(parsed_sql)
    checker = CmpDomainChecker(parsed_sql, columns)
    assert checker is not None
    warning_msg = checker.check()
    assert warning_msg is not None
