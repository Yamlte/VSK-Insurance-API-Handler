const axios = require('axios');
const { Driver, TokenAuthService, TypedValues } = require('ydb-sdk');
const { S3 } = require('aws-sdk');

const CONFIG = {
  YDB_TIMEOUT: 5000,
  VSK_API: {
    BASE_URL: 'https://services-stg.vsk.ru/ship/biz/sales/v2/',
    AUTH_URL: 'https://services-stg.vsk.ru/ship/token',
    TIMEOUT: 20000,
    RETRIES: 3,
  },
  OBJECT_STORAGE: {
    ENDPOINT: 'https://storage.yandexcloud.net',
    BUCKET: 'your-bucket-name',
    FOLDER: 'policy-previews/'
  }
};

let driverInstance;

function checkEnv(...vars) {
  for (const key of vars) {
    if (!process.env[key]) {
      console.error(`[checkEnv] Missing env: ${key}`);
      throw new Error(`Missing env: ${key}`);
    }
  }
}

async function fetchIamToken() {
  console.log('[fetchIamToken] Получение IAM токена из метаданных...');
  try {
    const resp = await axios.get(
      'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token',
      { headers: { 'Metadata-Flavor': 'Google' }, timeout: 2000 }
    );
    if (!resp.data || !resp.data.access_token) {
      throw new Error('Пустой токен в ответе');
    }
    return resp.data.access_token;
  } catch (err) {
    console.error('[fetchIamToken] Ошибка получения токена:', err.message);
    throw new Error('Не удалось получить IAM токен автоматически');
  }
}

async function getDriver() {
  console.log('[getDriver] Инициализация YDB драйвера...');
  if (!process.env.YDB_ENDPOINT || !process.env.YDB_DATABASE) {
    console.error('[getDriver] Не заданы переменные окружения YDB_ENDPOINT и YDB_DATABASE');
    throw new Error('Не заданы параметры YDB');
  }

  if (!driverInstance) {
    const iamToken = await fetchIamToken();
    driverInstance = new Driver({
      endpoint: process.env.YDB_ENDPOINT,
      database: process.env.YDB_DATABASE,
      authService: new TokenAuthService(iamToken),
    });
    const ready = await driverInstance.ready(5000);
    if (!ready) {
      console.error('[getDriver] YDB не готов');
      throw new Error('YDB not ready');
    }
    console.log('[getDriver] YDB драйвер готов');
  }

  return driverInstance;
}


async function getVskToken() {
  console.log('[getVskToken] Получение токена от ВСК...');
  checkEnv('VSK_CLIENT_ID', 'VSK_CLIENT_SECRET');

  for (let i = 0; i < CONFIG.VSK_API.RETRIES; i++) {
    try {
      const resp = await axios.post(
        CONFIG.VSK_API.AUTH_URL,
        new URLSearchParams({
          grant_type: 'client_credentials',
          client_id: process.env.VSK_CLIENT_ID,
          client_secret: process.env.VSK_CLIENT_SECRET,
        }),
        {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          timeout: CONFIG.VSK_API.TIMEOUT,
        }
      );
      console.log('[getVskToken] Токен ВСК получен');
      return resp.data.access_token;
    } catch (err) {
      console.error(`[getVskToken] Ошибка авторизации (попытка ${i+1}):`, err.response?.data || err.message);
      if (i === CONFIG.VSK_API.RETRIES - 1) throw err;
      await new Promise(r => setTimeout(r, 1000 * (i + 1)));
    }
  }
}

function parseRequestBody(event) {
  console.log('[parseRequestBody] Разбор тела запроса...');
  if (!event.body) return null;
  let raw = event.body;
  if (event.isBase64Encoded) raw = Buffer.from(raw, 'base64').toString('utf-8');
  try {
    return JSON.parse(raw);
  } catch (err) {
    console.error('[parseRequestBody] Error parsing body:', err.message);
    return null;
  }
}

/*function validateAccidentInput(params) {
  console.log('[validateAccidentInput] Валидация входных параметров...');
  const errors = [];
  if (!params.product?.code) errors.push('Не указан код продукта');
  if (!params.startDate) errors.push('Не указана дата начала');
  if (!params.endDate) errors.push('Не указана дата окончания');
  if (!params.issueDate) errors.push('Не указана дата выпуска');

  const ph = params.policyHolder?.person;
  if (!ph) {
    errors.push('Не указаны данные держателя полиса');
  } else {
    if (!ph.firstName) errors.push('Не указано имя держателя');
    if (!ph.lastName)  errors.push('Не указана фамилия держателя');
    if (!ph.birthDate) errors.push('Не указана дата рождения держателя');
  }

  const ins = params.insuredObject?.insureds?.[0]?.person;
  if (!ins) {
    errors.push('Не указаны данные застрахованного');
  } else {
    if (!ins.firstName) errors.push('Не указано имя застрахованного');
    if (!ins.lastName)  errors.push('Не указана фамилия застрахованного');
    if (!ins.birthDate) errors.push('Не указана дата рождения застрахованного');
  }

  const sum = params.insuredObject?.covers?.[0]?.sumInsured;
  if (![50000, 100000, 250000, 500000].includes(Number(sum))) {
    errors.push('Недопустимая сумма страхования');
  }

  if (errors.length) {
    console.error('[validateAccidentInput] Ошибки валидации:', errors);
    return errors;
  }
  console.log('[validateAccidentInput] Валидация успешна');
  return null;
}
*/
function buildAccidentRequest(input) {
  console.log('[buildAccidentRequest] Формирование запроса в ВСК...');
  const now = new Date();
  const moscow = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Moscow' }));
  const today = moscow.toISOString().split('T')[0];
  const issueDate = `${today}T23:59:59+03:00`;

  return {
    product:   input.product || { code: 'ACCIDENT' },
    startDate: input.startDate,
    endDate:   input.endDate,
    issueDate,
    policyHolder: {
      person:  { ...input.policyHolder.person, type: 'individual' },
      address: input.policyHolder.address,
      phone:   input.policyHolder.phone,
      email:   input.policyHolder.email,
    },
    insuredObject: {
      covers:  input.insuredObject.covers.map(c => ({ sumInsured: Number(c.sumInsured) || 50000 })),
      insureds: input.insuredObject.insureds.map(i => ({
        person:            { ...i.person, type: 'individual' },
        additionalFactors: i.additionalFactors,
      })),
    },
  };
}

async function handleCalc(session, token, params) {
  console.log('[handleCalc] Start');
  const url = `${CONFIG.VSK_API.BASE_URL}individual/accident/quotes`;
  const req = buildAccidentRequest(params);
  const { data } = await axios.post(url, req, {
    headers: { Authorization: `Bearer ${token}` },
    timeout: CONFIG.VSK_API.TIMEOUT,
  });
  
  const q = `
    PRAGMA TablePathPrefix("${process.env.YDB_DATABASE}");
    DECLARE $ts AS Timestamp;
    DECLARE $price AS Double;
    DECLARE $reqData AS Utf8;
    DECLARE $respData AS Utf8;
    UPSERT INTO insurance_calculations (timestamp, price, request_data, response_data)
    VALUES ($ts, $price, $reqData, $respData);
  `;
  await session.executeQuery(q, {
    $ts:       TypedValues.timestamp(new Date()),
    $price:    TypedValues.double(data.premium || 0),
    $reqData:  TypedValues.utf8(JSON.stringify(req)),
    $respData: TypedValues.utf8(JSON.stringify(data)),
  });
  return {
    statusCode: 200,
    body: JSON.stringify({
      premium:   data.premium,
      requestId: data.draftId,
      covers:    data.insuredObject.covers,
    }),
  };
}

async function handlePay(session, token, params) {
  console.log('[handlePay] Start');
  const base = `${CONFIG.VSK_API.BASE_URL}individual/accident/policies`;
  const req  = buildAccidentRequest(params);

  // 1. Создать полис
  const { data: policy } = await axios.post(base, req, {
    headers: { Authorization: `Bearer ${token}` },
    timeout: CONFIG.VSK_API.TIMEOUT,
  });

  // 2. Выполнить оплату первого взноса
  const payUrl = `${base}/${policy.policyNumber}/installments/1`;
  const { data: pay } = await axios.put(payUrl, {
    amount:      policy.premium,
    paymentType: 'CARD',
    successUrl:  params.successUrl,
    failUrl:     params.failUrl,
  }, {
    headers: { Authorization: `Bearer ${token}` },
    timeout: CONFIG.VSK_API.TIMEOUT,
  });

  // UPSERT в insurance_payments
  const q = `
    PRAGMA TablePathPrefix("${process.env.YDB_DATABASE}");
    DECLARE $ts AS Timestamp;
    DECLARE $polNo AS Utf8;
    DECLARE $prem  AS Double;
    DECLARE $link  AS Utf8;
    DECLARE $ext   AS Utf8;
    DECLARE $rdata AS Utf8;
    UPSERT INTO insurance_payments (timestamp, policyNumber, premium, paymentLink, extId, request_data)
    VALUES ($ts, $polNo, $prem, $link, $ext, $rdata);
  `;
  await session.executeQuery(q, {
    $ts:     TypedValues.timestamp(new Date()),
    $polNo:  TypedValues.utf8(policy.policyNumber),
    $prem:   TypedValues.double(policy.premium),
    $link:   TypedValues.utf8(pay.paymentLink),
    $ext:    TypedValues.utf8(params.id || ''),
    $rdata:  TypedValues.utf8(JSON.stringify(req)),
  });

  return {
    statusCode: 200,
    body: JSON.stringify({
      paymentLink:  pay.paymentLink,
      policyNumber: policy.policyNumber,
      premium:      policy.premium,
      id:           params.id,
    }),
  };
}



async function handleCreatePolicy(session, token, params) {
  console.log('[handleCreatePolicy] Создание полиса без оплаты...');
  const base = `${CONFIG.VSK_API.BASE_URL}individual/accident/policies`;
  const req = buildAccidentRequest(params);

  // Создаем полис
  const { data: policy } = await axios.post(base, req, {
    headers: { Authorization: `Bearer ${token}` },
    timeout: CONFIG.VSK_API.TIMEOUT,
  });

  console.log('[handleCreatePolicy] Полис создан. Номер:', policy.policyNumber);
  return {
    policyNumber: policy.policyNumber,
    premium: policy.premium,
    draftId: policy.draftId,
  };
}

async function handleSample(session, token, params) {
  console.log('[handleSample] Начало обработки');

  // 1. Создаем временный полис
  const policyData = await handleCreatePolicy(session, token, params);
  const { policyNumber, premium } = policyData;

  // 2. Получаем PDF от ВСК
  const pdfUrl = `${CONFIG.VSK_API.BASE_URL}policies/${policyNumber}/files/POLICY`;
  const pdfResponse = await axios.get(pdfUrl, {
    headers: { Authorization: `Bearer ${token}` },
    timeout: CONFIG.VSK_API.TIMEOUT,
  });

  if (!pdfResponse.data.policyPDF) {
    throw new Error('PDF не найден в ответе ВСК');
  }

  // 3. Сохраняем в Object Storage
  const s3 = new S3({
    endpoint: CONFIG.OBJECT_STORAGE.ENDPOINT,
    credentials: {
      accessKeyId: process.env.S3_ACCESS_KEY,
      secretAccessKey: process.env.S3_SECRET_KEY,
    },
  });

  const fileKey = `policy-samples/${policyNumber}.pdf`;
  await s3.putObject({
    Bucket: CONFIG.OBJECT_STORAGE.BUCKET,
    Key: fileKey,
    Body: Buffer.from(pdfResponse.data.policyPDF, 'base64'),
    ContentType: 'application/pdf',
  }).promise();

  // 4. Генерируем временную ссылку
  const signedUrl = s3.getSignedUrl('getObject', {
    Bucket: CONFIG.OBJECT_STORAGE.BUCKET,
    Key: fileKey,
    Expires: 3600, // Ссылка действительна 1 час
  });

  return {
    statusCode: 200,
    body: JSON.stringify({
      policyNumber,
      premium,
      pdfUrl: signedUrl, // Ссылка для скачивания
      expiresAt: new Date(Date.now() + 3600 * 1000).toISOString(),
    }),
  };
}


async function handlePdf(session, token, body) {
  console.log('[handlePdf] Загрузка PDF для полиса', body.policy);

  if (!body.policy) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: 'policy missing' })
    };
  }

  try {
    const response = await axios.get(`https://api.vsk.ru/v3/accident/policies/${body.policy}/pdf`, {
      headers: {
        Authorization: `Bearer ${token}`
      },
      responseType: 'arraybuffer'
    });

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/pdf',
        'Content-Disposition': `attachment; filename="${body.policy}.pdf"`
      },
      body: response.data.toString('base64'),
      isBase64Encoded: true
    };
  } catch (error) {
    console.error('[handlePdf] Ошибка загрузки PDF:', error.response?.data || error.message);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Ошибка при получении PDF', details: error.message })
    };
  }
}


exports.handler = async function(event, context) {
  console.log('[handler] Запуск функции-обработчика');
  const body = parseRequestBody(event);
  if (!body?.action) {
    return { 
      statusCode: 400, 
      body: JSON.stringify({ error: 'action missing' }) 
    };
  }

  try {
    const driver = await getDriver();
    return driver.tableClient.withSession(async session => {
      const token = await getVskToken();

      switch (body.action) {
  case 'calc':
    return handleCalc(session, token, body);

  case 'pay':
    return handlePay(session, token, body);

  case 'sample':
    return handleSample(session, token, body);

  case 'pdf':
    return handlePdf(session, token, body);

  default:
    return { 
      statusCode: 400, 
      body: JSON.stringify({ error: 'Unknown action' }) 
    };
}
    });
  } catch (e) {
    console.error('[handler] Internal error:', e);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: e.message })
    };
  }
};

// Отдельный обработчик для скачивания PDF
exports.pdfHandler = async function(event, context) {
  console.log('[pdfHandler] Запрос PDF', event);
  
  try {
    // Извлекаем номер полиса из пути
    const path = event.path || '';
    const policyNumber = path.split('/').pop().replace('.pdf', '');
    
    if (!policyNumber) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Policy number is required' })
      };
    }

    const driver = await getDriver();
    return await driver.tableClient.withSession(async session => {
      const token = await getVskToken();
      
      console.log(`[pdfHandler] Получение PDF для полиса ${policyNumber}`);
      const url = `${CONFIG.VSK_API.BASE_URL}policies/${policyNumber}/files/POLICY`;
      const response = await axios.get(url, {
        headers: { Authorization: `Bearer ${token}` },
        responseType: 'json',
        timeout: CONFIG.VSK_API.TIMEOUT,
      });

      if (!response.data.policyPDF) {
        return {
          statusCode: 404,
          body: JSON.stringify({ error: 'PDF not found' })
        };
      }

      const pdfData = Buffer.from(response.data.policyPDF, 'base64');
      
      return {
        statusCode: 200,
        headers: { 
          'Content-Type': 'application/pdf',
          'Content-Disposition': `attachment; filename=${policyNumber}.pdf`
        },
        body: pdfData.toString('base64'),
        isBase64Encoded: true
      };
    });
  } catch (e) {
    console.error('[pdfHandler] Error:', e);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: e.message })
    };
  }
};
