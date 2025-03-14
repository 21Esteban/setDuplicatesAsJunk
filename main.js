const mysql = require('mysql2/promise');

(async () => {
    const connection = await mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: '12345678',
        database: 'sfco_crm_perf'
    });

    console.log("✅ Conectado a la base de datos MySQL!");

    try {
        // 📌 Obtener todos los leads
        const [rows] = await connection.execute("SELECT * FROM tblleads WHERE junk = 0");

        const checkeds = [];

        for (const lead of rows) {
            if (checkIfAlreadyChecked(checkeds, lead)) continue;

            // 📌 Buscar todos los leads duplicados
            const [duplicates] = await connection.execute(
                `SELECT * FROM tblleads WHERE name = ? AND address LIKE CONCAT('%', ?, '%') AND phonenumber = ?`,
                [lead.name, lead.address, lead.phonenumber]
            );

            if (duplicates.length > 1) {
                console.log(`🔍 Encontrados ${duplicates.length} duplicados para: ${lead.name}`);

                // 📌 Calcular puntajes para ordenar
                const scoredDuplicates = duplicates.map(duplicate => {
                    let filledFieldsCount = Object.values(duplicate).filter(v => v !== null && v !== "" && v !== "none" && v !== 0).length;
                    let statusScore = duplicate.status !== 13 ? 5 : 0; // 📌 Si no es "new", +5 puntos
                    return { ...duplicate, score: filledFieldsCount + statusScore };
                });

                // 📌 Ordenamos los duplicados por mayor puntaje
                const sortedDuplicates = scoredDuplicates.sort((a, b) => b.score - a.score);

                const leadToKeep = sortedDuplicates[0]; // 📌 Nos quedamos con el mejor lead

                checkeds.push(leadToKeep); // 📌 Aseguramos que este queda sin `junk = 1`

                console.log(`✅ Manteniendo como válido: ID ${leadToKeep.id}`);

                // 📌 Marcar los demás como `junk = 1`
                for (let i = 1; i < sortedDuplicates.length; i++) {
                    await connection.execute(
                        `UPDATE tblleads SET junk = 1 WHERE id = ?`,
                        [sortedDuplicates[i].id]
                    );
                    console.log(`⚠️ Marcado como junk: ID ${sortedDuplicates[i].id}`);
                }
            } else {
                // 📌 Si no hay duplicados, lo agregamos como válido
                checkeds.push(lead);
            }
        }

        console.log("✅ Proceso de limpieza de duplicados completado.");
    } catch (error) {
        console.error("❌ Error:", error);
    } finally {
        await connection.end();
        console.log("Conexión cerrada.");
    }
})();

const checkIfAlreadyChecked = (array, object) => {
    return array.some(item =>
        item.name === object.name &&
        item.address === object.address &&
        item.phonenumber === object.phonenumber
    );
};
